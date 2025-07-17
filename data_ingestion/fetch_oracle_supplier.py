import pandas as pd
import oracledb
from config import oracle_config
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_8")
from datetime import datetime



class DataFetcherOracleSupplier:
    
    def __init__(self,psswrd,user_name,dsn):
        self.connection = None
        self.cursor = None
        self.psswrd=psswrd
        self.user_name=user_name
        self.dsn=dsn

    def connect(self):
        try:
            self.connection = oracledb.connect(
                user=self.user_name,
                password=self.psswrd,
                dsn=self.dsn
            )
            self.cursor = self.connection.cursor()
            print("connection established")
        except Exception as e:
            raise ConnectionError(f"❌ Failed to connect to Oracle DB: {e}")    



    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
                

   



    def fetch_invoice_data(self,date:str)-> pd.DataFrame:
        try:
            if not self.connection:
                raise ConnectionError("Database connection is not established.")    
            
            self.cursor.execute("""
                    BEGIN
                        mo_global.set_policy_context('S','82');
                    END;
                    """)
            self.connection.commit() 
            today = datetime.now().strftime('%d-%b-%Y').upper()

            query = f"""
                        SELECT
                            AP.INVOICE_DATE "Invoice Date",
                            AP.GL_DATE "Trx Date",
                            apv.VENDOR_TYPE_DISP, AP.VENDOR_ID, AP.VENDOR_SITE_ID, AP.VENDOR_NUMBER,
                            AP.VENDOR_NAME "Supplier Name",
                            APV.VAT_registration_num "VAT No",
                            AP.INVOICE_NUM " Invoice Number",
                            AP.SUPPLIER_TAX_INVOICE_NUMBER  "Control Unit Invoice Number",
                            (SELECT USER_NAME FROM FND_USER WHERE USER_ID = AP.CREATED_BY) "Created By",
                            AP.DESCRIPTION " Item Description",
                            AP.INVOICE_CURRENCY_CODE "Invoice Currency",
                            AP.EXCHANGE_RATE "Exchange Rate",
                            AP.CREDITED_INVOICE_NUM,
                            AP.PO_NUMBER,
                            AP.RECEIPT_NUMBER,
                            AP.DISTRIBUTION_TOTAL,
                            CASE WHEN AP.DISTRIBUTION_TOTAL = 0 THEN 'N' ELSE 'Y' END "Distribution Flag",
                            CASE WHEN AP.INVOICE_ID IN (
                                SELECT DISTINCT PK1_VALUE FROM FND_ATTACHED_DOCUMENTS WHERE ENTITY_NAME = 'AP_INVOICES'
                            ) THEN 'Yes' ELSE 'No' END "Attachments",
                            CASE WHEN (AP.TOTAL_TAX_AMOUNT <> 0)
                                THEN (AP.ACTUAL_INVOICE_AMOUNT - AP.TOTAL_TAX_AMOUNT)
                                ELSE AP.ACTUAL_INVOICE_AMOUNT END "Exclusive",
                            AP.TOTAL_TAX_AMOUNT "Tax",
                            AP.ACTUAL_INVOICE_AMOUNT "Inclusive",
                            CASE WHEN (AP.INVOICE_CURRENCY_CODE = 'USD')
                                THEN ((AP.ACTUAL_INVOICE_AMOUNT - AP.TOTAL_TAX_AMOUNT) * AP.EXCHANGE_RATE)
                                ELSE (AP.ACTUAL_INVOICE_AMOUNT - AP.TOTAL_TAX_AMOUNT) END "Exclusive KSH",
                            CASE WHEN (AP.INVOICE_CURRENCY_CODE = 'USD')
                                THEN (AP.TOTAL_TAX_AMOUNT * AP.EXCHANGE_RATE)
                                ELSE AP.TOTAL_TAX_AMOUNT END "Tax KSH",
                            CASE WHEN (AP.INVOICE_CURRENCY_CODE = 'USD')
                                THEN (AP.ACTUAL_INVOICE_AMOUNT * AP.EXCHANGE_RATE)
                                ELSE AP.ACTUAL_INVOICE_AMOUNT END "Inclusive KSH"
                        FROM
                            AP_invoices_V AP,
                            AP_vendors_v APV
                        WHERE
                            TO_DATE(AP.GL_DATE,'DD-MON-RRRR') >= TO_DATE('{date}', 'DD-MM-RRRR')
                            AND TO_DATE(AP.GL_DATE,'DD-MON-RRRR') <= TO_DATE('{today}', 'DD-MM-RRRR')
                            AND AP.VENDOR_NUMBER = APV.VENDOR_NUMBER
                        """

            df = self.cursor.execute(query)
            rows = df.fetchall()
            columns = [desc[0] for desc in self.cursor.description]  # Get column n
            data = pd.DataFrame(rows, columns=columns)
            data['invoiceUniqueKey']=data['VENDOR_ID'].astype(str)+"_"+data[' Invoice Number']
            final_data=data[['invoiceUniqueKey','Invoice Date','VENDOR_ID',' Invoice Number','Inclusive KSH']]
           
            
            
            cleaned=final_data.rename(columns={
                                                ' Invoice Number': 'invoiceNumber',
                                                'Invoice Date': 'invoiceDate',
                                                'VENDOR_ID': 'vendorId',
                                                'Inclusive KSH':'invoiceGrossValue'
                                            })
            return cleaned.drop_duplicates()
            
        

        except Exception as e:
            raise RuntimeError(f"❌ Failed to fetch invoice data: {e}")

        


        


    def fetch_supplier_data(self,vendor_ids:list) -> pd.DataFrame :
        vendor_ids=tuple(int(i) for i in vendor_ids)
        try:
            if not self.connection:
                raise ConnectionError("Database connection is not established.")

            if not vendor_ids:
                raise ValueError("Customer number list is empty.")    
        


            query=f"""select
aps.vendor_id   "SUPPLIER ID",aps.END_DATE_ACTIVE,
aps.vendor_name "SUPPLIER NAME",aps.CREATION_DATE,fu.USER_NAME,
aps.segment1  "SUPPLIER NUMBER",
-------
--PER.EMPLOYEE_NUMBER,PER.GLOBAL_NAME,pp.D_TERMINATION_DATE,PER.PERSON_ID,
--------
aps.vendor_type_lookup_code "SUPPLIER TYPE",
apss.VENDOR_SITE_ID,
apss.VENDOR_SITE_CODE "SUPPLIER SITE",
apss.CREATE_DEBIT_MEMO_FLAG,--added by Godfrey 28-06-2019  RTV issue
aps.CREATE_DEBIT_MEMO_FLAG,  --added by Godfrey 28-06-2019  RTV issue
aps.terms_id ,
aps.vat_registration_num  "TAX NUMBER",
DECODE(aps.auto_tax_calc_flag, 'N', 'No', 'Y', 'Yes', aps.auto_tax_calc_flag) "AutoCalculate Tax Flag",
apss.ALLOW_AWT_FLAG,
aps.party_id,
apt.name "PAY TERMS",
PVC.title,
pvc.prefix||' '||pvc.first_name||' '||pvc.middle_name||' '||pvc.last_name "CONTACT NAME",
pvc.mail_stop,
pvc.area_code,
pvc.phone,
pvc.email_address,
pvc.alt_area_code,
pvc.alt_phone,
pvc.fax_area_code,
pvc.fax,
apss.address_line1,
apss.address_line2,
apss.address_line3,
apss.city,
apss.state,
apss.zip,
apss.country,
apss.invoice_currency_code "INV CURR",
apss.payment_currency_code "PAY CURR",
-------------------------
(select bc.BANK_CHARGE_BEARER from IBY_EXTERNAL_PAYEES_ALL bc --AP_SUPPLIER_SITES_ALL apss
where apss.VENDOR_SITE_ID = bc.SUPPLIER_SITE_ID)"BANK_CHARGE_BEARER",
----------------------------
(SELECT
ieb.bank_name
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ieb.bank_party_id = iebb.bank_party_id
and ieba.branch_id = iebb.branch_party_id
and ieba.bank_id = ieb.bank_party_id
and ROWNUM=1)"BANK  NAME",

(SELECT
iebb.bank_branch_name
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ieb.bank_party_id = iebb.bank_party_id
and ieba.branch_id = iebb.branch_party_id
and ieba.bank_id = ieb.bank_party_id
and ROWNUM=1)"BANK BRANCH NAME",
--------
(SELECT
iebb.BRANCH_NUMBER
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ieb.bank_party_id = iebb.bank_party_id
and ieba.branch_id = iebb.branch_party_id
and ieba.bank_id = ieb.bank_party_id
and ROWNUM=1)"BRANCH NUM",
----------
(SELECT
iebb.EFT_SWIFT_CODE
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ieb.bank_party_id = iebb.bank_party_id
and ieba.branch_id = iebb.branch_party_id
and ieba.bank_id = ieb.bank_party_id
and ROWNUM=1)"EFT_BIC",
(SELECT
ieba.BANK_ACCOUNT_NUM
--ieba.BANK_ACCOUNT_NAME "BANK ACCOUNT NAME"
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ROWNUM=1)"BANK ACCOUNT NUMBER",
(SELECT
ieba.BANK_ACCOUNT_NAME "BANK ACCOUNT NAME"
FROM
apps.iby_ext_bank_accounts ieba,
apps.iby_account_owners iao,
apps.iby_ext_banks_v ieb,
apps.iby_ext_bank_branches_v iebb
WHERE 1=1
and aps.vendor_id = apss.vendor_id
and iao.account_owner_party_id = aps.party_id
and ieba.ext_bank_account_id = iao.ext_bank_account_id
and ROWNUM=1)"BANK ACCOUNT NAME",
aps.MATCH_OPTION,receipt_required_flag,aps.INSPECTION_REQUIRED_FLAG,
DECODE (NVL(APS.receipt_required_flag, 'N'),
'Y', DECODE(NVL(APS.inspection_required_flag, 'N'),
'Y', '4-Way',
'3-Way'),
'2-Way') matching_Level
from ap_suppliers aps,
ap_supplier_sites_all apss,FND_USER FU,
po_vendor_contacts pvc,ap_terms apt
-------------------

-------------------------
where aps.vendor_id=apss.vendor_id(+)
and apss.vendor_id=pvc.vendor_id(+)
and aps.TERMS_ID=apt.TERM_ID(+)
AND aps.vendor_type_lookup_code <> 'EMPLOYEE'
-------------------------
--AND APS.EMPLOYEE_ID = PER.PERSON_ID
--AND APS.EMPLOYEE_ID = Pp.PERSON_ID
-----------------------------
--AND APS.EMPLOYEE_ID ='688'
--AND aps.VENDOR_NAME = NVL (:P_VENDOR_NAME, aps.VENDOR_NAME)
--AND pp.D_TERMINATION_DATE IS NULL
--and apss.ALLOW_AWT_FLAG is null
--and aps.END_DATE_ACTIVE is null
--and apss.CREATE_DEBIT_MEMO_FLAG = 'N'
--and aps.CREATE_DEBIT_MEMO_FLAG = 'N'
--AND aps.CREATION_DATE BETWEEN NVL (:p_from_date, aps.CREATION_DATE)
--AND NVL (:p_to_date, aps.CREATION_DATE)+1 
and aps.LAST_UPDATED_BY = fu.USER_ID
and aps.vendor_id in {vendor_ids}
--and apt.name is null --Pay Terms

order by aps.segment1 """
            df = self.cursor.execute(query)
            rows = df.fetchall()
            print("Sucessfully fetched data")
            columns = [desc[0] for desc in self.cursor.description]  # Get column names

            
            data = pd.DataFrame(rows, columns=columns)
            
        
            
            data=data[['SUPPLIER ID','SUPPLIER NAME','SUPPLIER TYPE','PAY TERMS', 'PHONE', 'EMAIL_ADDRESS']]
            print(data.columns)
            cleaned = data.rename(columns={
                                            'SUPPLIER ID': 'vendorId',
                                            'SUPPLIER NAME': 'supplierName',
                                            'EMAIL_ADDRESS': 'email',
                                            'PHONE': 'phone',
                                            'SUPPLIER TYPE': 'supplierType',
                                            'PAY TERMS': 'creditTerms'
                                        })
            
            print(cleaned.columns)
            return cleaned.drop_duplicates()
            

        except Exception as e:
            raise RuntimeError(f"❌ Failed to fetch customer data: {e}")
      
      
# class DataFetcherOracleSupplier:

   

#     def fetch_invoice_data(self,date):

#         data=pd.read_excel('storage/invoice supplier.xls')
#         data['invoiceUniqueKey']=data['VENDOR_ID'].astype(str)+"_"+data[' Invoice Number']
#         final_data=data[['invoiceUniqueKey','Invoice Date','VENDOR_ID',' Invoice Number','Inclusive KSH']]
#         final_data=final_data[final_data['Invoice Date']>date]
#         final_data=final_data[final_data['Invoice Date']<'12-02-2024']

        
        
#         cleaned=final_data.rename(columns={
#                                             ' Invoice Number': 'invoiceNumber',
#                                             'Invoice Date': 'invoiceDate',
#                                             'VENDOR_ID': 'vendorId',
#                                             'Inclusive KSH':'invoiceGrossValue'
#                                         })
#         return cleaned.drop_duplicates()

        


        


#     def fetch_supplier_data(self,vendor_ids:list):
#         data=pd.read_excel('storage/data2.xls')
        
#         data=data[['SUPPLIER ID','SUPPLIER NAME','SUPPLIER TYPE','PAY TERMS', 'PHONE', 'EMAIL_ADDRESS']]
#         print(data.columns)
#         data = data.rename(columns={
#                                         'SUPPLIER ID': 'vendorId',
#                                         'SUPPLIER NAME': 'supplierName',
#                                         'EMAIL_ADDRESS': 'email',
#                                         'PHONE': 'phone',
#                                         'SUPPLIER TYPE': 'supplierType',
#                                         'PAY TERMS': 'creditTerms'
#                                     })
#         cleaned=data[data['vendorId'].isin(vendor_ids)]
#         print(cleaned.columns)
#         return cleaned.drop_duplicates()
