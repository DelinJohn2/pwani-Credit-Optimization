from database.models.supplier import Suppliers
from database.models.supplierPayment import SupplierPayment
from database.sql_update.base import BaseUpdater
from utils import setup_logger

logger=setup_logger('sql_updater_supplier')




class CreateSupplierData(BaseUpdater):


    def __init__(self,engine):
        super().__init__(engine)
        
    
    def supplier_metadata_insert(self,db):
        try:    
            self.instances = []
            for index,i in db.iterrows():
                print(i['vendorId'])
                data=Suppliers(
                    vendorId=i['vendorId'],
                    name=i['supplierName'],
                    email=i['email'],
                    phone=i['phone'],
                    supplierType=i['supplierType'],
                    creditTerms=i['creditTerms']
                )
                
                self.instances.append(data)
            if self.instances:
                    self.bulk_insert()
                    logger.info(f"Inserted {len(self.instances)} customer metadata records.")    
            else:
                    logger.info("No customer metadata records to insert.")        
            self.bulk_insert()
        except Exception as e:
            logger.error(f"supplier_metadata_insert failed: {str(e)}")
            raise



    def supplier_offer_insert(self,db):
        try:    
            self.instances= []
            for index,i in db.iterrows():
                data=SupplierPayment(
                    invoiceUniqueKey=i['invoiceUniqueKey'],

                    invoiceDate=i['invoiceDate'],
                    vendorId=i['vendorId'],

                    invoiceNumber=i['invoiceNumber'],

                    supplierId=i['supplierId'],

                    creditAmount=i['invoiceGrossValue'],
                    discountRate=i['discountRate'],
                    
                    originalPaymentDate=i['orginalPaymentDate'],
                    offeredPaymentDate=i['offeredPaymentDate'],
                    offerStatus="not_sent",
                    
                    
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

