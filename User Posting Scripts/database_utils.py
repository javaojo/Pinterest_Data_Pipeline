import sqlalchemy
import creds


class AWSDBConnector:

    def __init__(self):
        # RDS database connection details
        self.HOST = creds.HOST
        self.USER = creds.USER
        self.PASSWORD = creds.PASSWORD
        self.DATABASE = creds.DATABASE
        self.PORT = creds.PORT

    def create_db_connector(self):
        # Create a SQLAlchemy engine for database connection
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
