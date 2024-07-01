import pandas

from airflow.providers.redis.hooks.redis import RedisHook as BaseRedisHook


class RedisHook(BaseRedisHook):

    def df_to_hset(self, df: pandas.DataFrame, hset_name: str, column_name: str, value_name: str = None):
        redis_client = self.get_conn()
        redis_client.delete(hset_name)
        print("Clean old hset successfully")
        print(f"Start write {df.shape[0]} customer_id")
        count = 0
        for index, row in df.iterrows():
            if value_name is None or value_name == '':
                redis_client.hset(hset_name, key=row[column_name], value=1)
            else:
                redis_client.hset(
                    hset_name, key=row[column_name], value=int(row[value_name]))
            count += 1
            if count % 100 == 0:
                print(f"Wrote {count} customer_id ")

        print(f"Write hset {hset_name} successfully")
