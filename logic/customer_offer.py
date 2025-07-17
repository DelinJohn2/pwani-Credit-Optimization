import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import setup_logger
from data_ingestion import DataFetcherLocalCustomer,DataFetcherOracleCustomer
from database import CreateCustomerData
from sqlmodel import create_engine,Session
from config import oracle_config


logger=setup_logger("Customer_offer")





from sqlmodel import Session
import pandas as pd
from typing import Dict

class CustomerDataManager:
    def __init__(self, engine):
        try:    
        
            self.engine = engine
            dsn,username,password=oracle_config()
            self.oracle = DataFetcherOracleCustomer(dsn=dsn,user_name=username,psswrd=password)
            # self.oracle=DataFetcherOracleCustomer()
            self.local = DataFetcherLocalCustomer(engine)
            self.customer_creator = CreateCustomerData(engine)
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise    
        


    def ensure_customers_in_db(self, customer_nos: list):
        try:
            existing = self.local.fetch_customer_no()
            missing = list(set(customer_nos) - set(existing))
            if missing:
                oracle_customer_df = self.oracle.fetch_customer_data(missing)
                self.customer_creator.customer_metadata_insert(oracle_customer_df)
            full_customers = self.local.fetch_customer_data()
            return full_customers
        except Exception as e:
            logger.error(f"ensure_customers_in_db failed: {str(e)}")
            raise
    

    

    def get_enriched_invoice_data(self, date: str) -> pd.DataFrame:


        try:    
    
            invoice_df = self.oracle.fetch_invoice_data(date)
            print(invoice_df.columns)
            customer_nos = list(set(invoice_df["customerNumber"]))
            print(customer_nos)
            customer_data = self.ensure_customers_in_db(customer_nos)
            return invoice_df.merge(customer_data.reset_index(), on='customerKey', how='left')
        except Exception as e:
            logger.error(f"get_enriched_invoice_data failed: {str(e)}")
            raise




    def offer_processor(self,input_data: dict) -> pd.DataFrame:
        

        try:
            

            date = self.local.fetch_last_invoice_date()
            
            if date:
                date =f'{date.strftime("%d-%m-%Y")}'
                print(date)
            else:
                date = (datetime.now() - relativedelta(months=1)).strftime("%d-%m-%Y")
            self.oracle.connect()    
            unfiltered_data = self.get_enriched_invoice_data(date) 
            invoice_nos_str=self.local.fetch_invoice_no()


            self.oracle.disconnect()


            invoice_no=[int(i) for i in invoice_nos_str]
            data = unfiltered_data[~unfiltered_data['invoiceNumber'].isin(invoice_no)]
            

            if not data.empty:
                data=data[data['creditTerms'].str.contains('Days Net',regex=True,na=False,case=False)]

            

           
                data['creditTerms'] = data['creditTerms'].apply(lambda x: int(x.split(" ")[0]))

                data=data[data['creditTerms']>7]
                
            
                data['invoiceDate'] = pd.to_datetime(data['invoiceDate'])
                
                data['orginalPaymentDate'] = data['invoiceDate'] + pd.to_timedelta(data['creditTerms'], unit="d")
                print(data['creditTerms'].unique())
                data['creditAmount'] = data.apply(
                        lambda x: x['invoiceGrossValue'] * input_data['exchange_rate'] if x['invoiceCurrencyCode'] == "USD" else x['invoiceGrossValue'],
                        axis=1
                    )
                
                data['saved_amount']=data.apply(
                        lambda x:(input_data.get(x['creditTerms']))*(input_data.get('cost_of_finance_per_day')/100)*x['creditAmount'],
                        axis=1
                        )
                    
                data['max_allowed_offer'] = data['creditAmount'] * (input_data.get('max_discount_amount') / 100)
                data['totalInterest']=data['creditTerms']*input_data.get("cost_of_finance_per_day")/100*data['creditAmount']

                data['discountRate'] = np.where(
                        data['saved_amount'] * (input_data.get('persentage_of_discount_savings') / 100)<data['max_allowed_offer'],
                        data['saved_amount'] * (input_data.get('persentage_of_discount_savings')/ 100),
                        data['max_allowed_offer']

                    )

                    
                data["offeredPaymentDate"] = data.apply(
                        lambda x: x['orginalPaymentDate'] - pd.to_timedelta(input_data.get(x['creditTerms'], 0), unit="d"),
                        axis=1
                    )
             

        
                self.customer_creator.customer_offer_insert(data)
                return data
            
            return "already inserted"
                        


        


        except Exception as e:
            logger.error(f"output formatter stopped :{str(e)}")
            raise
        