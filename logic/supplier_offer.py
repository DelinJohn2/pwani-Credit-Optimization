import pandas as pd
import numpy as np
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
from utils import setup_logger
from data_ingestion import DataFetcherLocalSupplier,DataFetcherOracleSupplier
from database import CreateSupplierData
from sqlmodel import create_engine,Session
from config import oracle_config

import re
logger=setup_logger("supplier_offer")
class SupplierDataManger:
    def __init__(self,engine):

        try:
            dsn, username, password =oracle_config()
            self.engine=engine
            self.oracle=DataFetcherOracleSupplier(password,username,dsn)
            # self.oracle = DataFetcherOracleSupplier()
            self.local =DataFetcherLocalSupplier(engine)
            self.supplier_creator=CreateSupplierData(engine)
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise         




    def ensure_supplier_in_db(self,vendor_ids:list):
        try:
            existing = self.local.fetch_vendor_ids()
            missing =list(set(vendor_ids)-set(existing))
            if missing:
                oracle_supplier_df =self.oracle.fetch_supplier_data(missing)
                
                self.supplier_creator.supplier_metadata_insert(oracle_supplier_df)
            full_supplier = self.local.fetch_supplier_data() 

            return full_supplier
        except Exception as e:
            logger.error(f"ensure_customers_in_db failed: {str(e)}")
            raise 


    def enriched_invoice_data(self, last_date:str):

        try:
            invoice_df=self.oracle.fetch_invoice_data(last_date)
            vendor_ids= tuple(invoice_df['vendorId'])  
            supplier_data =self.ensure_supplier_in_db(vendor_ids)

            return pd.merge(invoice_df,supplier_data,on='vendorId', how='left')
        

        except Exception as e:
            logger.error(f"get_enriched_invoice_data failed: {str(e)}")
            raise


    def extract_credit_days(self,text):
        match = re.search(r'\d+', str(text))
        return int(match.group()) if match else None    

    def offer_processor(self,input_data: dict):

        try:
            self.oracle.connect()

            last_date=self.local.fetch_last_invoice_date()

            if last_date:
                last_date =  f'{last_date.strftime("%d-%m-%Y")}'#'12-06-2023'
            else:
                last_date = (datetime.now() - relativedelta(months=2)).strftime("%d-%m-%Y")#'12-06-2023'

            unfiltered_data = self.enriched_invoice_data(last_date)
            invoice_key = self.local.fetch_invoice_key()

            self.oracle.disconnect()

            data=unfiltered_data[~unfiltered_data['invoiceUniqueKey'].isin(invoice_key)]


            if not data.empty:
                data= data[data['creditTerms'].str.contains("Days Net", regex=True, na=False,case=False)]
                


                data['creditTerms'] = data['creditTerms'].apply(self.extract_credit_days)
                


                data = data[data['creditTerms']>7]

                data['invoiceDate'] = pd.to_datetime(data['invoiceDate'])
                data['orginalPaymentDate'] = data['invoiceDate'] + pd.to_timedelta(data['creditTerms'], unit="d")

                data['discountAmount']=data.apply(
                    lambda x:(input_data.get(x['creditTerms'],(0, 0))[1]/100)*x['invoiceGrossValue'],axis=1)
                
                data['discountRate']=data['invoiceGrossValue']-data['discountAmount']

                data['discountPercentage']=data['creditTerms'].apply(lambda x:(input_data.get(x,(0, 0))[1]))

                data["offeredPaymentDate"] = data.apply(
                    lambda x : x['orginalPaymentDate'] - pd.to_timedelta(input_data.get(x['creditTerms'],(0, 0))[0], unit="d"),axis=1
                )
                print("Data is being inserted")

                self.supplier_creator.supplier_offer_insert(data)

                return "data insertion is successfull"

            
            return "already inserted"
        

        
        except Exception as e:
            logger.error(f"output formatter stopped :{str(e)}")
            raise