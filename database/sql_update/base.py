

from sqlmodel import Session,select
from utils import setup_logger
logger=setup_logger("sql_update_base")


class BaseUpdater:
    def __init__(self,engine):
        
        self.instances=[]
        self.engine=engine
     

    def bulk_insert(self):
            with Session(self.engine) as session:
                    session.add_all(self.instances)
                    session.commit()  



def unit_insert(target_value, value, datamodel, column_name,engine):
        with Session(engine) as session:
            # Fetch the row
            result= session.exec(select(datamodel).where(datamodel.invoice_number== target_value))
        
            table= result.one()
            if table:
                
                setattr(table, column_name, value)
                
                session.add(table)
                session.commit()
                return True
            else:
                return False  # Row not found

