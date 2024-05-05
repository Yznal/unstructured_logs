import clickhouse_connect
from datetime import datetime
from nltk.stem import PorterStemmer, WordNetLemmatizer
import nltk

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
nltk.download('wordnet')
sentence = """Finished Spring Data repository scanning in 26 ms. Found 2 JPA repository interfaces."""
txt = "HikariPool-<:NUM:> - Thread starvation or clock leap detected (housekeeper <:*:>"
stemmer = PorterStemmer()
lemmatizer = WordNetLemmatizer()
tokens = nltk.word_tokenize(sentence)
stemmed_words = [stemmer.stem(word) for word in tokens]
print(stemmed_words)

lemmatized_words = [lemmatizer.lemmatize(word) for word in tokens]
print(lemmatized_words)

tagged = nltk.pos_tag(tokens)
print(tagged)