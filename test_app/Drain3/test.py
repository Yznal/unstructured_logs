import clickhouse_connect
from datetime import datetime
from nltk.stem import PorterStemmer, WordNetLemmatizer
import nltk
import re

""" client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('''CREATE TABLE IF NOT EXISTS otel_logs
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
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1''') """
time1 = '2024-04-27T23:02:09.511+03:00'
time2 = '2024-04-27T23:45:31.929+03:00'
row1 = [datetime.strptime(time1, "%Y-%m-%dT%H:%M:%S.%f%z"), 'INFO', 9, 'Finished Spring Data repository scanning in 26 ms. Found 2 JPA repository interfaces.', {'Thread': 'main', 'Logger': 'o.s.s.petclinic.PetClinicApplication', 'EventTemplate':'Finished Spring Data repository scanning in <:NUM:> ms. Found <:NUM:> JPA repository interfaces.'}]
row2 = [datetime.strptime(time2, "%Y-%m-%dT%H:%M:%S.%f%z"), 'WARN', 13,  'HikariPool-1 - Thread starvation or clock leap detected (housekeeper delta=58s379ms).', {'Thread': 'main', 'Logger': 'o.apache.catalina.core.StandardEngine', 'EventTemplate':'HikariPool-<:NUM:> - Thread starvation or clock leap detected (housekeeper delta=58s379ms).'}]
data = [row1, row2]
#client.insert('otel_logs', data, column_names=['Timestamp', 'SeverityText', 'SeverityNumber', 'Body', 'LogAttributes'])
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')
#nltk.download('wordnet')
txt3 = """Finished Spring Data repository scanning in 26 ms. Found 2 JPA repository interfaces."""
txt = "HikariPool <:NUM:> - Added connection conn0: url=<:EQL:> user=<:EQL:> added"
txt2 = "Starting Servlet engine: [Apache Tomcat <:VER:>]"
txt4 = "HHH000412: Hibernate ORM core version <:VER:>.Final"
txt5 = "HikariPool-1 - Added connection conn0: url=jdbc:h2:mem:533cfc55-d44b-41d1-a3a8-4cc10daa7ebb user=SA"
txt6 = "Added <:NUM:> added added"
txt7 = "Started PetClinicApplication in <:TIME:> (added added added <:FLT:>)"
txt8 = "HikariPool-1 - <:*:> <:*:>"

x = re.sub("""([^\w\s*])""", " ", txt8)
line = re.sub("""\s+""", " ", x)

print("AFTER REGEX ---", line)

stemmer = PorterStemmer()
lemmatizer = WordNetLemmatizer()
tokens = nltk.word_tokenize(line)
params_list = ["NUM", "ID", "IP", "VER", "EQL", "FLT", "SEQ", "HEX", "CMD", "TIME", "*"]
tags = ["NN", "NNP", "NNS", "NNPS", "JJ", "JJR", "JJS"]
lower_tokens = []
for token in tokens:
    if token not in params_list:
        lower_tokens.append(token.lower())
    else:
        lower_tokens.append(token)
print("LOWER TOKENS ---", lower_tokens)
regex = "((?<=[=])\\S+)"
stemmed_words = [stemmer.stem(word) for word in tokens]
#print(stemmed_words)

lemmatized_words = [lemmatizer.lemmatize(word) for word in tokens]
#print(lemmatized_words)

tagged = nltk.pos_tag(lower_tokens)
print("TAGGED ---", tagged)
params_pos = []

ans = []

for pos, val in enumerate(tagged):
    if val[1] == "IN":
        tagged.pop(pos)
for pos, val in enumerate(tagged):
    if val[0] in params_list:
        params_pos.append(pos)
print("TAGGED 2 ---", tagged)
print(params_pos)
it = 1000
cnt = 0
buf = -1
try:
    for pos, val in enumerate(tagged):
        if val[0] == "EQL":
            it = 1000
            ans.append(buf)
            buf = -1
            cnt += 1
            continue
        if val[0] in params_list:
            continue
        if cnt > len(params_pos) - 1:
            break
        if val[1] not in tags and (pos - params_pos[cnt]) >= 2 and buf != -1:
            it = 1000
            ans.append(buf)
            cnt += 1
            buf = -1
            raise Exception("Sorry, no numbers below zero")
        elif val[1] in tags and ((pos - params_pos[cnt]) > 2 or abs(pos - params_pos[cnt]) >= it):
            it = 1000
            ans.append(buf)
            cnt += 1
            if cnt <= len(params_pos) - 1:
                it = abs(pos - params_pos[cnt])
            buf = pos
            raise Exception("Sorry, no numbers below zero")
        elif val[1] in tags and abs(pos - params_pos[cnt]) < it:
            it = abs(pos - params_pos[cnt])
            buf = pos
    print(cnt)
    if cnt != len(params_pos):
        ans.append(buf)
except Exception as e:
    print(e)
    for i in range(len(params_pos) - cnt):
         ans.append(-1)
print(params_pos)
print(ans)
params_name = []
for i in ans:
    if i == -1:
        params_name.append('')
    else:
        params_name.append(tagged[i][0])
print(params_name)

params_dict = {}
params_dict["A"] = 1
params_dict["C"] = 4
params_dict["B"] = 3
print(params_dict)
for key, value in params_dict.items():
    print(key, value)
print("A" in params_dict)