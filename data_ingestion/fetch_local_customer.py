from sqlmodel import Session, select, func
from database import Customer, CustomerOffer
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from utils import setup_logger


logger=setup_logger("fetch_local_customer")


class DataFetcherLocalCustomer:
    def __init__(self, engine):
        self.engine = engine

    def fetch_customer_no(self):
        try:
            with Session(self.engine) as session:
                stmt = select(Customer.customerNumber)
                result = session.exec(stmt).fetchall()
                return result
        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch customer numbers from local DB: {str(e)}")
            raise RuntimeError(f"Failed to fetch customer numbers from local DB: {str(e)}") from e

    def fetch_customer_data(self):
        try:
            with Session(self.engine) as session:
                stmt = select(Customer)
                result = pd.read_sql_query(stmt, self.engine)
                return result
        except Exception as e:
            logger.error((f"Failed to fetch customer data from local DB: {str(e)}"))
            raise RuntimeError(f"Failed to fetch customer data from local DB: {str(e)}") from e

    def fetch_invoice_no(self):
        try:
            with Session(self.engine) as session:
                stmt = select(CustomerOffer.invoiceNumber)
                result = session.exec(stmt).fetchall()
                return result
        except SQLAlchemyError as e:
            logger.error((f"Failed to fetch invoice numbers from local DB: {str(e)}"))
            raise RuntimeError(f"Failed to fetch invoice numbers from local DB: {str(e)}") from e

    def fetch_last_invoice_date(self):
        try:
            with Session(self.engine) as session:
                stmt = select(func.max(CustomerOffer.invoiceDate))
                result = session.exec(stmt).one()
                return result
        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch the latest invoice date from local DB: {str(e)}")
            raise RuntimeError(f"Failed to fetch the latest invoice date from local DB: {str(e)}") from e
