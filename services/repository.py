from psycopg2.extras import execute_batch
import logging


class Repository:
    def __init__(self, connection):
        self.connection = connection

    def init_db(self):
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date date NOT NULL,
                        campaign_id text NOT NULL,
                        spend numeric,
                        conversions integer,
                        cpa numeric,
                        PRIMARY KEY (date, campaign_id)
                    );
                """)
            self.connection.commit()
            logging.info("Table 'daily_stats' created or already exists")

        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            raise


    def upsert_stats(self, rows):
        query = """
            INSERT INTO daily_stats (date, campaign_id, spend, conversions, cpa)
            VALUES (%(date)s, %(campaign_id)s, %(spend)s, %(conversions)s, %(cpa)s)
            ON CONFLICT (date, campaign_id)
            DO UPDATE SET
                spend = EXCLUDED.spend,
                conversions = EXCLUDED.conversions,
                cpa = EXCLUDED.cpa;
        """
        with self.connection.cursor() as cur:
            execute_batch(cur, query, rows)
        self.connection.commit()