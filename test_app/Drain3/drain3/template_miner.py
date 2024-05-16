# SPDX-License-Identifier: MIT

import base64
import logging
import re
import time
import zlib
import nltk
import clickhouse_connect
from datetime import datetime
from typing import Optional, List, NamedTuple

import regex as re
import os
import pandas as pd

import jsonpickle
from cachetools import LRUCache, cachedmethod

from drain3.drain import Drain, LogCluster
from drain3.jaccard_drain import JaccardDrain
from drain3.masking import LogMasker
from drain3.persistence_handler import PersistenceHandler
from drain3.simple_profiler import SimpleProfiler, NullProfiler, Profiler
from drain3.template_miner_config import TemplateMinerConfig


logger = logging.getLogger(__name__)

config_filename = 'drain3.ini'

ExtractedParameter = NamedTuple("ExtractedParameter", [("value", str), ("mask_name", str)])


class TemplateMiner:

    def __init__(self,
                 persistence_handler: PersistenceHandler = None,
                 config: TemplateMinerConfig = None, log_format: str = "",
                 log_file: str = "", output_dir: str = "", clickhouse: bool = False,
                 headers_mapping: dict = None, time_format: str = ""):
        """
        Wrapper for Drain with persistence and masking support
        :param persistence_handler: The type of persistence to use. When None, no persistence is applied.
        :param config: Configuration object. When none, configuration is loaded from default .ini file (if exist)
        """
        logger.info("Starting Drain3 template miner")

        if config is None:
            logger.info(f"Loading configuration from {config_filename}")
            config = TemplateMinerConfig()
            config.load(config_filename)

        self.config = config

        self.profiler: Profiler = NullProfiler()

        if self.config.profiling_enabled:
            self.profiler = SimpleProfiler()

        self.persistence_handler = persistence_handler

        param_str = f"{self.config.mask_prefix}*{self.config.mask_suffix}"

        # Follow the configuration in the configuration file to instantiate Drain
        # target_obj will be "Drain" if the engine argument is not specified.
        target_obj = self.config.engine
        if target_obj not in ["Drain", "JaccardDrain"]:
            raise ValueError(f"Invalid matched_pattern: {target_obj}, must be either 'Drain' or 'JaccardDrain'")

        self.drain = globals()[target_obj](
            sim_th=self.config.drain_sim_th,
            depth=self.config.drain_depth,
            max_children=self.config.drain_max_children,
            max_clusters=self.config.drain_max_clusters,
            extra_delimiters=self.config.drain_extra_delimiters,
            profiler=self.profiler,
            param_str=param_str,
            parametrize_numeric_tokens=self.config.parametrize_numeric_tokens
        )

        self.masker = LogMasker(self.config.masking_instructions, self.config.mask_prefix, self.config.mask_suffix)
        self.parameter_extraction_cache = LRUCache(self.config.parameter_extraction_cache_capacity)
        self.last_save_time = time.time()

        if clickhouse:
            self.clickhouse = clickhouse_connect.get_client(host='host.docker.internal', username='default', password='')
            print("Successfull conection to clickhouse!!!!!")

        self.df_log = None
        self.use_clickhouse = clickhouse
        self.log_format = log_format
        self.rex = None
        self.path = log_file
        self.output_dir = output_dir
        self.headers_mapping = headers_mapping
        self.time_format = time_format
        self.headers = None

        if persistence_handler is not None:
            self.load_state()

    def load_data(self, log_message: str):
        headers, regex = self.generate_logformat_regex(self.log_format)
        self.headers = headers
        self.df_log = self.log_to_dataframe(
            log_message, regex, headers
        )

    def log_to_dataframe(self, log_line, regex, headers):
            """Function to transform log file to dataframe"""
            log_messages = []
            try:
                match = regex.search(log_line.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
            except Exception as e:
                print("[Warning] Skip line: " + log_line)
            logdf = pd.DataFrame(log_messages, columns=headers)
            logdf.insert(0, "LineId", None)
            if self.drain.print_headers:
                logdf["LineId"] = 1
            else:
                df = pd.read_csv(os.path.join(self.output_dir, self.path + "_structured.csv"))
                logdf["LineId"] = df.iloc[-1]["LineId"] + 1
            return logdf

    def generate_logformat_regex(self, logformat):
            """Function to generate regular expression to split log messages"""
            headers = []
            splitters = re.split(r"(<[^<>]+>)", logformat)
            regex = ""
            for k in range(len(splitters)):
                if k % 2 == 0:
                    splitter = re.sub(" +", "\\\s+", splitters[k])
                    regex += splitter
                else:
                    header = splitters[k].strip("<").strip(">")
                    regex += "(?P<%s>.*?)" % header
                    headers.append(header)
            regex = re.compile("^" + regex + "$")
            return headers, regex


    def output_result(self, cluster, change_type):
            df_events = []
            if change_type == "cluster_created":
                df_events.append([cluster.cluster_id, cluster.get_template(), 1])
            elif change_type == "none":
                df = pd.read_csv(os.path.join(self.output_dir, self.path + "_templates.csv"))
                df.loc[df["EventId"] == cluster.cluster_id, "Occurrences"] += 1
                df.to_csv(os.path.join(self.output_dir, self.path + "_templates.csv"), index=False, mode="w")
            elif change_type == "cluster_template_changed":
                df = pd.read_csv(os.path.join(self.output_dir, self.path + "_templates.csv"))
                df.loc[df["EventId"] == cluster.cluster_id, "Occurrences"] += 1
                df.loc[df["EventId"] == cluster.cluster_id, "EventTemplate"] = cluster.get_template()
                df.to_csv(os.path.join(self.output_dir, self.path + "_templates.csv"), index=False, mode="w")

            df_event = pd.DataFrame(
                df_events, columns=["EventId", "EventTemplate", "Occurrences"]
            )
            self.df_log["EventId"] = cluster.cluster_id
            self.df_log["EventTemplate"] = cluster.get_template()
            self.df_log["ParameterList"] = [self.get_parameters_name(cluster)]
            self.df_log.to_csv(os.path.join(self.output_dir, self.path + "_structured.csv"),index=False,
                               mode='a' if not self.drain.print_headers else 'w', header=self.drain.print_headers
            )
            
            df_event.to_csv(
                os.path.join(self.output_dir, self.path + "_templates.csv"),
                index=False,
                mode='a' if not self.drain.print_headers else 'w',
                header=self.drain.print_headers,
                columns=["EventId", "EventTemplate", "Occurrences"],
            )

            
            if self.use_clickhouse:
                self.create_table()
                self.export_to_clickhouse()
            #print("PARAMETERS-----", self.extract_parameters(cluster.get_template(), self.df_log.iloc[0]["Content"], exact_matching=False))
            self.drain.print_headers = False


    def get_parameters_name(self, cluster):
        no_symb = re.sub("""([^\w\s*])""", " ", cluster.get_template())
        no_space = re.sub("""\s+""", " ", no_symb)
        tokens = nltk.word_tokenize(no_space)
        params_list = ["NUM", "ID", "IP", "VER", "EQL", "FLT", "SEQ", "HEX", "CMD", "TIME", "*"]
        tags = ["NN", "NNP", "NNS", "NNPS", "JJ", "JJR", "JJS"]
        lower_tokens = []

        for token in tokens:
            if token not in params_list:
                lower_tokens.append(token.lower())
            else:
                lower_tokens.append(token)
        tagged = nltk.pos_tag(lower_tokens)

        params_pos = []
        names_pos = []

        for pos, val in enumerate(tagged):
            if val[1] == "IN":
                tagged.pop(pos)
        for pos, val in enumerate(tagged):
            if val[0] in params_list:
                params_pos.append(pos)
        
        dist = 1000
        cnt = 0
        nearest_pos = -1
        try:
            for pos, val in enumerate(tagged):
                if val[0] == "EQL":
                    dist = 1000
                    names_pos.append(nearest_pos)
                    nearest_pos = -1
                    cnt += 1
                    continue
                if val[0] in params_list:
                    continue
                if cnt > len(params_pos) - 1:
                    break
                if val[1] not in tags and (pos - params_pos[cnt]) >= 2 and nearest_pos != -1:
                    dist = 1000
                    names_pos.append(nearest_pos)
                    nearest_pos = -1
                    cnt += 1
                elif val[1] in tags and ((pos - params_pos[cnt]) > 2 or abs(pos - params_pos[cnt]) >= dist):
                    dist = 1000
                    names_pos.append(nearest_pos)
                    cnt += 1
                    if cnt <= len(params_pos) - 1:
                        dist = abs(pos - params_pos[cnt])
                    nearest_pos = pos
                elif val[1] in tags and abs(pos - params_pos[cnt]) < dist:
                    dist = abs(pos - params_pos[cnt])
                    nearest_pos = pos
            if cnt != len(params_pos):
                names_pos.append(nearest_pos)
        except Exception as e:
            logger.error(e)
        for i in range(len(params_pos) - cnt):
                names_pos.append(-1)
        params_name_list = []
        for i in names_pos:
            if i == -1:
                params_name_list.append('')
            else:
                params_name_list.append(tagged[i][0])
        params_names = {}
        for i, param in enumerate(self.get_parameter_list(cluster.get_template(), self.df_log.iloc[0]["Content"])):
            if params_name_list[i] == '':
                params_names[f"Parameter {i}"] = param
            else:
                params_names[params_name_list[i]] = param
        return params_names

    def create_table(self):
        self.clickhouse.command('''CREATE TABLE IF NOT EXISTS otel_logs
        (
            `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
            `TraceId` String CODEC(ZSTD(1)),
            `SpanId` String CODEC(ZSTD(1)),
            `TraceFlags` UInt32 CODEC(ZSTD(1)),
            `SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
            `SeverityNumber` Int32 CODEC(ZSTD(1)),
            `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
            `Body` String CODEC(ZSTD(1)),
            `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            `LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
            INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        PARTITION BY toDate(Timestamp)
        ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
        SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1''')


    def export_to_clickhouse(self):
        level_map = {"TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21}
        level = self.df_log.iloc[0][self.headers_mapping["Level"]]
        if level in level_map:
            level_number = level_map[level]
        else:
            level_number = 9
        log_attributes = {'EventTemplate': self.df_log.iloc[0]["EventTemplate"],
                          'EventId': str(self.df_log.iloc[0]["EventId"])}
        for header in self.headers:
            if header not in self.headers_mapping:
                log_attributes[header] = str(self.df_log.iloc[0][header])
        for key, value in self.df_log.iloc[0]["ParameterList"].items():
            log_attributes[key] = value
        data = [datetime.strptime(self.df_log.iloc[0][self.headers_mapping["Timestamp"]],
                                    self.time_format),
                                    level,
                                    level_number,
                                    self.df_log.iloc[0][self.headers_mapping["Content"]],
                                    log_attributes]
        self.clickhouse.insert('otel_logs', [data], column_names=['Timestamp', 'SeverityText', 'SeverityNumber', 'Body', 'LogAttributes'])


    def load_state(self):
        logger.info("Checking for saved state")

        state = self.persistence_handler.load_state()
        if state is None:
            logger.info("Saved state not found")
            return

        if self.config.snapshot_compress_state:
            state = zlib.decompress(base64.b64decode(state))

        loaded_drain: Drain = jsonpickle.loads(state, keys=True)

        # json-pickle encoded keys as string by default, so we have to convert those back to int
        # this is only relevant for backwards compatibility when loading a snapshot of drain <= v0.9.1
        # which did not use json-pickle's keys=true
        if len(loaded_drain.id_to_cluster) > 0 and isinstance(next(iter(loaded_drain.id_to_cluster.keys())), str):
            loaded_drain.id_to_cluster = {int(k): v for k, v in list(loaded_drain.id_to_cluster.items())}
            if self.config.drain_max_clusters:
                cache = LRUCache(maxsize=self.config.drain_max_clusters)
                cache.update(loaded_drain.id_to_cluster)
                loaded_drain.id_to_cluster = cache
        self.drain.print_headers = loaded_drain.print_headers
        self.drain.id_to_cluster = loaded_drain.id_to_cluster
        self.drain.clusters_counter = loaded_drain.clusters_counter
        self.drain.root_node = loaded_drain.root_node

        logger.info(f"Restored {len(loaded_drain.clusters)} clusters "
                    f"built from {loaded_drain.get_total_cluster_size()} messages")

    def save_state(self, snapshot_reason):
        state = jsonpickle.dumps(self.drain, keys=True).encode('utf-8')
        if self.config.snapshot_compress_state:
            state = base64.b64encode(zlib.compress(state))

        logger.info(f"Saving state of {len(self.drain.clusters)} clusters "
                    f"with {self.drain.get_total_cluster_size()} messages, {len(state)} bytes, "
                    f"reason: {snapshot_reason}")
        self.persistence_handler.save_state(state)

    def get_snapshot_reason(self, change_type, cluster_id):
        if change_type != "none":
            return f"{change_type} ({cluster_id})"

        diff_time_sec = time.time() - self.last_save_time
        if diff_time_sec >= self.config.snapshot_interval_minutes * 60:
            return "periodic"

        return None

    def add_log_message(self, log_message: str) -> dict:
        self.profiler.start_section("total")

        self.load_data(log_message)
        self.profiler.start_section("mask")
        masked_content = self.masker.mask(self.df_log.iloc[0]["Content"])
        self.profiler.end_section()

        self.profiler.start_section("drain")
        cluster, change_type = self.drain.add_log_message(masked_content)
        self.profiler.end_section("drain")

        self.output_result(cluster, change_type)

        result = {
            "change_type": change_type,
            "cluster_id": cluster.cluster_id,
            "cluster_size": cluster.size,
            "template_mined": cluster.get_template(),
            "cluster_count": len(self.drain.clusters)
        }

        if self.persistence_handler is not None:
            self.profiler.start_section("save_state")
            snapshot_reason = self.get_snapshot_reason(change_type, cluster.cluster_id)
            if snapshot_reason:
                self.save_state(snapshot_reason)
                self.last_save_time = time.time()
            self.profiler.end_section()

        self.profiler.end_section("total")
        self.profiler.report(self.config.profiling_report_sec)
        return result

    def match(self, log_message: str, full_search_strategy="never") -> LogCluster:
        """
        Mask log message and match against an already existing cluster.
        Match shall be perfect (sim_th=1.0).
        New cluster will not be created as a result of this call, nor any cluster modifications.

        :param log_message: log message to match
        :param full_search_strategy: when to perform full cluster search.
            (1) "never" is the fastest, will always perform a tree search [O(log(n)] but might produce
            false negatives (wrong mismatches) on some edge cases;
            (2) "fallback" will perform a linear search [O(n)] among all clusters with the same token count, but only in
            case tree search found no match.
            It should not have false negatives, however tree-search may find a non-optimal match with
            more wildcard parameters than necessary;
            (3) "always" is the slowest. It will select the best match among all known clusters, by always evaluating
            all clusters with the same token count, and selecting the cluster with perfect all token match and least
            count of wildcard matches.
        :return: Matched cluster or None if no match found.
        """

        masked_content = self.masker.mask(log_message)
        matched_cluster = self.drain.match(masked_content, full_search_strategy)
        return matched_cluster

    def get_parameter_list(self, log_template: str, log_message: str) -> List[str]:
        """
        Extract parameters from a log message according to a provided template that was generated
        by calling `add_log_message()`.

        This function is deprecated. Please use extract_parameters instead.

        :param log_template: log template corresponding to the log message
        :param log_message: log message to extract parameters from
        :return: An ordered list of parameter values present in the log message.
        """

        extracted_parameters = self.extract_parameters(log_template, log_message, exact_matching=False)
        if not extracted_parameters:
            return []
        return [parameter.value for parameter in extracted_parameters]

    def extract_parameters(self,
                           log_template: str,
                           log_message: str,
                           exact_matching: bool = True) -> Optional[List[ExtractedParameter]]:
        """
        Extract parameters from a log message according to a provided template that was generated
        by calling `add_log_message()`.

        For most accurate results, it is recommended that
        - Each `MaskingInstruction` has a unique `mask_with` value,
        - No `MaskingInstruction` has a `mask_with` value of `*`,
        - The regex-patterns of `MaskingInstruction` do not use unnamed back-references;
          instead use back-references to named groups e.g. `(?P=some-name)`.

        :param log_template: log template corresponding to the log message
        :param log_message: log message to extract parameters from
        :param exact_matching: whether to apply the correct masking-patterns to match parameters, or try to approximate;
            disabling exact_matching may be faster but may lead to situations in which parameters
            are wrongly identified.
        :return: A ordered list of ExtractedParameter for the log message
            or None if log_message does not correspond to log_template.
        """

        for delimiter in self.config.drain_extra_delimiters:
            log_message = re.sub(delimiter, " ", log_message)

        template_regex, param_group_name_to_mask_name = self._get_template_parameter_extraction_regex(
            log_template, exact_matching)

        # Parameters are represented by specific named groups inside template_regex.
        parameter_match = re.match(template_regex, log_message)

        # log template does not match template
        if not parameter_match:
            return None

        # create list of extracted parameters
        extracted_parameters = []
        for group_name, parameter in parameter_match.groupdict().items():
            if group_name in param_group_name_to_mask_name:
                mask_name = param_group_name_to_mask_name[group_name]
                extracted_parameter = ExtractedParameter(parameter, mask_name)
                extracted_parameters.append(extracted_parameter)

        return extracted_parameters

    @cachedmethod(lambda self: self.parameter_extraction_cache)
    def _get_template_parameter_extraction_regex(self, log_template: str, exact_matching: bool):
        param_group_name_to_mask_name = {}
        param_name_counter = [0]

        def get_next_param_name():
            param_group_name = f"p_{str(param_name_counter[0])}"
            param_name_counter[0] += 1
            return param_group_name

        # Create a named group with the respective patterns for the given mask-name.
        def create_capture_regex(_mask_name):
            allowed_patterns = []
            if exact_matching:
                # get all possible regex patterns from masking instructions that match this mask name
                masking_instructions = self.masker.instructions_by_mask_name(_mask_name)
                for mi in masking_instructions:
                    # MaskingInstruction may already contain named groups.
                    # We replace group names in those named groups, to avoid conflicts due to duplicate names.
                    if hasattr(mi, 'regex'):
                        mi_groups = mi.regex.groupindex.keys()
                        pattern = mi.pattern
                    else:
                        # non regex masking instructions - support only non-exact matching
                        mi_groups = []
                        pattern = ".+?"

                    for group_name in mi_groups:
                        param_group_name = get_next_param_name()

                        def replace_captured_param_name(param_pattern):
                            _search_str = param_pattern.format(group_name)
                            _replace_str = param_pattern.format(param_group_name)
                            return pattern.replace(_search_str, _replace_str)

                        pattern = replace_captured_param_name("(?P={}")
                        pattern = replace_captured_param_name("(?P<{}>")

                    # support unnamed back-references in masks (simple cases only)
                    pattern = re.sub(r"\\(?!0)\d{1,2}", r"(?:.+?)", pattern)
                    allowed_patterns.append(pattern)

            if not exact_matching or _mask_name == "*":
                allowed_patterns.append(r".+?")

            # Give each capture group a unique name to avoid conflicts.
            param_group_name = get_next_param_name()
            param_group_name_to_mask_name[param_group_name] = _mask_name
            joined_patterns = "|".join(allowed_patterns)
            capture_regex = f"(?P<{param_group_name}>{joined_patterns})"
            return capture_regex

        # For every mask in the template, replace it with a named group of all
        # possible masking-patterns it could represent (in order).
        mask_names = set(self.masker.mask_names)

        # the Drain catch-all mask
        mask_names.add("*")

        escaped_prefix = re.escape(self.masker.mask_prefix)
        escaped_suffix = re.escape(self.masker.mask_suffix)
        template_regex = re.escape(log_template)

        # replace each mask name with a proper regex that captures it
        for mask_name in mask_names:
            search_str = escaped_prefix + re.escape(mask_name) + escaped_suffix
            while True:
                rep_str = create_capture_regex(mask_name)
                # Replace one-by-one to get a new param group name for each replacement.
                template_regex_new = template_regex.replace(search_str, rep_str, 1)
                # Break when all replaces for this mask are done.
                if template_regex_new == template_regex:
                    break
                template_regex = template_regex_new

        # match also messages with multiple spaces or other whitespace chars between tokens
        template_regex = re.sub(r"\\ ", r"\\s+", template_regex)
        template_regex = f"^{template_regex}$"
        return template_regex, param_group_name_to_mask_name
