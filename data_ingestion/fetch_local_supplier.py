from sqlmodel import Session, select, func
from database import SupplierPayment,Suppliers
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from utils import setup_logger


logger=setup_logger("fetch_local_supplier")


class DataFetcherLocalSupplier:
    def __init__(self, engine):
        self.engine = engine

    def fetch_vendor_ids(self):
        try:
            with Session(self.engine) as session:
                stmt = select(Suppliers.vendorId)
                result = session.exec(stmt).fetchall()
                return result
        except SQLAlchemyError as e:
            logger.exception("Error fetching vendor IDs")
            raise RuntimeError(f"Failed to fetch customer numbers from local DB: {str(e)}") from e

    def fetch_supplier_data(self):
        try:
            with Session(self.engine) as session:
                stmt = select(Suppliers)
                result = pd.read_sql_query(stmt, self.engine)
                return result
        except Exception as e:
            logger.exception("Error fetching supplier data")
            raise RuntimeError(f"Failed to fetch customer data from local DB: {str(e)}") from e

    def fetch_invoice_key(self):
        try:
            with Session(self.engine) as session:
                stmt = select(SupplierPayment.invoiceUniqueKey)
                result = session.exec(stmt).fetchall()
                return result
        except SQLAlchemyError as e:
            logger.exception("Error fetching invoice keys")
            raise RuntimeError(f"Failed to fetch invoice numbers from local DB: {str(e)}") from e

    def fetch_last_invoice_date(self):
        try:
            with Session(self.engine) as session:
                stmt = select(func.max(SupplierPayment.invoiceDate))
                result = session.exec(stmt).one()
                return result
        except SQLAlchemyError as e:
            logger.exception("Error fetching last invoice date")
            raise RuntimeError(f"Failed to fetch the latest invoice date from local DB: {str(e)}") from e