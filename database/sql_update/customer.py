
from database.models.customer import Customer
from database.models.customerOffer import CustomerOffer
from database.sql_update.base import BaseUpdater
from utils import setup_logger

logger=setup_logger('sql_updater_customer')




class CreateCustomerData(BaseUpdater):


    def __init__(self,engine):
        super().__init__(engine)
        
    
    def customer_metadata_insert(self,db):
        try:    
            self.instances = []
            for index,i in db.iterrows():
                data=Customer(
                    customerNumber=i['customerNumber'],
                    name=i['name'],
                    email=i['email'],
                    customerType=i['customerType'],
                    creditTerms=i['creditTerms'],
                    customerKey=i['customerKey']
                )
                
                self.instances.append(data)
            if self.instances:
                    self.bulk_insert()
                    logger.info(f"Inserted {len(self.instances)} customer metadata records.")    
            else:
                    logger.info("No customer metadata records to insert.")        
            self.bulk_insert()
        except Exception as e:
            logger.error(f"customer_metadata_insert failed: {str(e)}")
            raise



    def customer_offer_insert(self,db):
        try:    
            self.instances= []
            for index,i in db.iterrows():
                data=CustomerOffer(
                    invoiceDate=i['invoiceDate'],
                    invoiceNumber=i['invoiceNumber'],
                    customerId=i['customerId'],
                    creditAmount=i['creditAmount'],
                    discountRate=i['discountRate'],
                    originalPaymentDate=i['orginalPaymentDate'],
                    offeredPaymentDate=i['offeredPaymentDate'],
                    totalInterest=i['totalInterest'],
                    offerStatus="not_sent",
                    customerKey=i['customerKey'],
                    discountPercentage=i['discountPercentage']
                    
                    
                    
                )

                self.instances.append(data)

            if self.instances:
                self.bulk_insert()
                logger.info(f"Inserted {len(self.instances)} customer offer records.")
            else:
                logger.info("No customer offer records to insert.")    
            self.bulk_insert()        
        except Exception as e:
            logger.error(f"customer_offer_insert failed: {str(e)}")
            raise

