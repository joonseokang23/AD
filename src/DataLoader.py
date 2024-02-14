import concurrent.futures

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config


class DataLoader:
    def __init__(self, endpoint_url="http://10.10.30.11:9000"):
        self._db_connection_info = {
            "dbname": "LGES_metadata",
            "user": "guardione",
            "password": "onepredict1!",
            "host": "10.10.30.25",
            "port": "5432",
        }
        self._minio_connection_info = {
            "endpoint_url": endpoint_url,  # "http://10.10.30.11:9000",
            "aws_access_key_id": "guardione",
            "aws_secret_access_key": "onepredict1!",
            "config": Config(signature_version="s3v4"),
            "verify": False,
        }
        self._max_threads = 10
        self.motor_info = self._get_motor_info()
        self.channel_info = self._get_channel_info()

    def query(self, **kwargs):
        """Query metadata.

        Args:
            site (str, optional): 공장명
            process (str, optional): 공정명
            building (str, optional): 빌딩명
            line (str, optional): 라인명
            equipment (str, optional): 호기명
            motor (str, optional): 모터명
            number (int, optional): 모터 번호
            channel: (str, optional): 채널명
            start_time: (datetime.datetime, optional): 조회 시작 시간
            end_time: (datetime.datetime, optional): 조회 끝 시간
            motor_id (int, optional): motor_info에서 찾은 motor_id
            channel_id (int, optional): channel_info에서 찾은 motor_id

        Return:
            query_results: List[Tuple(str, str, str)]
        """
        with psycopg2.connect(**self._db_connection_info) as connection:
            with connection.cursor() as cursor:
                statement = """select e.site, e.process, l.file_path
                            from public.acquisition_log as l
                            join public.motor as m on m.id = l.motor_id
                            join public.equipment as e on e.id = m.equipment_id
                            join public.acquisition_setting as s on s.id = l.setting_id
                            join public.channel as c on l.channel_id = c.id
                            where 1=1"""

                if "site" in kwargs:
                    statement = " ".join(
                        [statement, f"and e.site = '{kwargs['site']}'"]
                    )
                if "process" in kwargs:
                    statement = " ".join(
                        [statement, f"and e.process = '{kwargs['process']}'"]
                    )
                if "building" in kwargs:
                    statement = " ".join(
                        [statement, f"and e.building = '{kwargs['building']}'"]
                    )
                if "line" in kwargs:
                    statement = " ".join(
                        [statement, f"and e.line = '{kwargs['line']}'"]
                    )
                if "equipment" in kwargs:
                    statement = " ".join(
                        [statement, f"and e.name = '{kwargs['equipment']}'"]
                    )

                if "motor" in kwargs:
                    statement = " ".join(
                        [statement, f"and m.name = '{kwargs['motor']}'"]
                    )
                if "number" in kwargs:
                    statement = " ".join(
                        [statement, f"and m.number = '{kwargs['number']}'"]
                    )
                if "motor_id" in kwargs:
                    statement = " ".join(
                        [statement, f"and m.id = '{kwargs['motor_id']}'"]
                    )

                if "channel" in kwargs:
                    statement = " ".join(
                        [statement, f"and c.name = '{kwargs['channel']}'"]
                    )
                if "channel_id" in kwargs:
                    statement = " ".join(
                        [statement, f"and c.id = '{kwargs['channel_id']}'"]
                    )

                if "start_time" in kwargs:
                    statement = " ".join(
                        [
                            statement,
                            f"and l.acq_time >= to_timestamp('{kwargs['start_time']}', 'yyyy-mm-dd hh24:mi:ss')::timestamp",
                        ]
                    )
                if "end_time" in kwargs:
                    statement = " ".join(
                        [
                            statement,
                            f"and l.acq_time <= to_timestamp('{kwargs['end_time']}', 'yyyy-mm-dd hh24:mi:ss')::timestamp",
                        ]
                    )

                cursor.execute(statement)
                records = cursor.fetchall()
        return records

    def load(self, query_result):
        """Load each raw data using file path list.

        Args:
            query_result (Tuple[str, str, str]): query를 이용해 가져온 minio path가 담겨있는 리스트의 element
        """
        client = boto3.client("s3", **self._minio_connection_info)
        bucket = ".".join(["lges", query_result[0].strip(), query_result[1].strip()])
        return client.get_object(Bucket=bucket, Key=query_result[2])["Body"].read()

    def _load_each(self, query_result):
        client = boto3.client("s3", **self._minio_connection_info)
        bucket = ".".join(["lges", query_result[0].strip(), query_result[1].strip()])
        return (
            "_".join([record.strip() for record in query_result]),
            client.get_object(Bucket=bucket, Key=query_result[2])["Body"].read(),
        )

    def load_bulk(self, query_results):
        """Load bulk raw data using file path list.

        Args:
            query_results (List[str]): query를 이용해 가져온 minio path가 담겨있는 리스트
        """
        result = []
        for i in range(0, len(query_results), self._max_threads):
            chunk = query_results[i : i + self._max_threads]

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_threads
            ) as executor:
                result.extend(list(executor.map(self._load_each, chunk)))
        return result

    def query_from_raw_sql(self, statement):
        with psycopg2.connect(**self._db_connection_info) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement)
                records = cursor.fetchall()
        return records

    def _get_motor_info(self):
        with psycopg2.connect(**self._db_connection_info) as connection:
            with connection.cursor() as cursor:
                statement = """select m.id as motor_id, e.site as site, e.process as process,
                                e.line as line, e.name as equipment, m.name as motor_name,
                                m.number as motor_number
                            from public.motor as m
                            join public.equipment as e on e.id = m.equipment_id
                            """
                cursor.execute(statement)
                column_names = tuple(desc[0] for desc in cursor.description)
                records = cursor.fetchall()
        return pd.DataFrame(records, columns=column_names)

    def _get_channel_info(self):
        with psycopg2.connect(**self._db_connection_info) as connection:
            with connection.cursor() as cursor:
                statement = """select c.id, c.name
                            from public.channel as c
                            """
                cursor.execute(statement)
                column_names = tuple(desc[0] for desc in cursor.description)
                records = cursor.fetchall()
        return pd.DataFrame(records, columns=column_names)
