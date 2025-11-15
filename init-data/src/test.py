from config import (GlobalConfig,
                    _init_env)
from pprint import pprint
from utils import get_conn

_init_env()
conn = get_conn()

cursor = conn.cursor()

cursor.execute(query=f"""COPY datasource.company_status(company_status_time_update,
                                                        company_status_type,
                                                        company_status_active,
                                                        company_status_fk,
                                                        company_status_primary_exchange)
                        FROM '{GlobalConfig.COMPANY_STATUS_TABLE_PATH}'
                        DELIMITER ','
                        CSV HEADER;
                    """)
conn.commit()
print("Executed OK")
conn.close()