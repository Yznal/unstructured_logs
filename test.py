import csv
import pandas as pd
""" df = pd.read_csv('test_app/Drain/demo_result/spring.log_templates.csv')
#spamreader = csv.reader(csvfile)
df.loc[df["EventTemplate"] == "HikariPool-1 - Start completed.", "Occurrences"] += 10
print(df)
df.to_csv('test_app/Drain/demo_result/spring.log_templates.csv', index=False, mode="w")
print(df) """

df = pd.DataFrame({'animal': ["Closing JPA EntityManagerFactory for persistence unit 'default'", 'bee', 'falcon', 'lion',
                   'monkey', 'parrot', 'shark', 'whale', 'zebra']})
print(df['animal'].head(1))
    