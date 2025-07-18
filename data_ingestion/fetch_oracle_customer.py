import pandas as pd
import re
import oracledb
from utils import setup_logger
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_8")
logger=setup_logger("fetch_oracle_customer")


class DataFetcherOracleCustomer:
    
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
            logger.info("connection established")
        except Exception as e:
            logger.exception(f"Failed to connect to Oracle DB: {e}")
            raise ConnectionError(f"Failed to connect to Oracle DB: {e}")    



    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
                

    def normalize(self,text):
        if pd.isna(text):
            return ""
        text = text.strip().lower()
        text = re.sub(r'[^a-z0-9]', '', text)  # remove all non-alphanumeric characters
        return text



    def fetch_invoice_data(self,date):
        try:
            if not self.connection:
                raise ConnectionError("Database connection is not established.")    
        

            query=f"""SELECT  --NAME, CUSTOMER_NAME, ORDER_NUMBER,INVOICE_NUMBER, PERIOD  , INVOICE_DATE,  ,INVOICE_AMOUNT
    PB.* 
    ,(select ROUND(costvu.item_cost,5) from CST_ITEM_COST_TYPE_V costvu
    where costvu.INVENTORY_ITEM_ID = pb.INVENTORY_ITEM_ID
    AND pb.ORGANIZATION_ID = COSTVU.ORGANIZATION_ID
    AND costvu.item_cost<>0 and costvu.COST_TYPE_ID = 2)UNIT_COST
    -------------------------------------------
    ,(SELECT distinct MDEV.ELEMENT_VALUE FROM MTL_DESCR_ELEMENT_VALUES_V MDEV WHERE MDEV.INVENTORY_ITEM_ID=PB.INVENTORY_ITEM_ID AND MDEV.ELEMENT_NAME='Product Code' and rownum=1) "Product Code"
    ,(SELECT distinct  MDEV.ELEMENT_VALUE FROM MTL_DESCR_ELEMENT_VALUES_V MDEV WHERE MDEV.INVENTORY_ITEM_ID=PB.INVENTORY_ITEM_ID AND MDEV.ELEMENT_NAME='Product Pack'  and rownum=1) "Product Pack"
    ,
    --------
    (SELECT distinct MDEV.ELEMENT_VALUE FROM MTL_DESCR_ELEMENT_VALUES_V MDEV WHERE MDEV.INVENTORY_ITEM_ID=PB.INVENTORY_ITEM_ID AND MDEV.ELEMENT_NAME='Size' and rownum=1)   "SKU Size",
    (SELECT distinct MDEV.ELEMENT_VALUE FROM MTL_DESCR_ELEMENT_VALUES_V MDEV WHERE MDEV.INVENTORY_ITEM_ID=PB.INVENTORY_ITEM_ID AND MDEV.ELEMENT_NAME='Additional Description' and rownum=1) "Variant",
    (SELECT distinct MDEV.ELEMENT_VALUE FROM MTL_DESCR_ELEMENT_VALUES_V MDEV WHERE MDEV.INVENTORY_ITEM_ID=PB.INVENTORY_ITEM_ID AND MDEV.ELEMENT_NAME='Sub Variant' and rownum=1) "Sub Variant"
    --,SOLD_TO_CUSTOMER_ID,rcta.SOLD_TO_CUSTOMER_ID,
    ,ac.CUSTOMER_NUMBER "Customer No"
    FROM APPS.XX_PWANIBRAND_VIEW PB,ra_customer_trx_lines_all RCTL ,ra_customer_trx_all rcta,AR_CUSTOMERS AC
    WHERE 
    TRUNC(PB.INVOICE_DATE) >= TO_DATE('{date}', 'DD-MM-YYYY')
    and PB.customer_trx_id = rctl.customer_trx_id(+)
    and PB.CUSTOMER_TRX_LINE_ID = rctl.LINK_TO_CUST_TRX_LINE_ID (+)
    and PB.customer_trx_id = rcta.CUSTOMER_TRX_ID (+)
    and rcta.SOLD_TO_CUSTOMER_ID = ac.CUSTOMER_ID (+)"""
            df = self.cursor.execute(query)
            rows = df.fetchall()
            columns = [desc[0] for desc in self.cursor.description]  # Get column n
            data = pd.DataFrame(rows, columns=columns)
            logger.info(f"fetched invoice data with shape {data.shape}")

            data['customerKey']=data.apply(
                lambda x:self.normalize(x['SHIP_TO_LOCATIONS1'])+'_'+str(x['Customer No']),axis=1
                                            )
            
        
            final_data=data[['INVOICE_NUMBER','INVOICE_DATE','Customer No','INVOICE_GROSS_VALUE','INVOICE_CURRENCY_CODE','customerKey']]
            
            cleaned=final_data.groupby(['INVOICE_NUMBER']).agg({
                                                            'INVOICE_DATE': 'first',
                                                            'Customer No': 'first',
                                                            'INVOICE_GROSS_VALUE': 'sum',
                                                            'INVOICE_CURRENCY_CODE': 'first',
                                                            'customerKey':'first'

                                                            }).reset_index()
            cleaned=cleaned.rename(columns={
                                                'INVOICE_NUMBER': 'invoiceNumber',
                                                'INVOICE_DATE': 'invoiceDate',
                                                'Customer No': 'customerNumber',
                                                'INVOICE_GROSS_VALUE': 'invoiceGrossValue',
                                                'INVOICE_CURRENCY_CODE':'invoiceCurrencyCode'
                                            })
            logger.info(f"sucessfully returned data data columns are {cleaned.columns} with shape {cleaned.shape}")
            return cleaned
            
        

        except Exception as e:
            logger.exception(f"Failed to fetch invoice data: {e}")

            raise RuntimeError(f"Failed to fetch invoice data: {e}")

        


        


    def fetch_customer_data(self,customer_nos:list):
        
        try:
            customer_no=",".join(map(str,customer_nos))
            if not self.connection:
                logger.error("Database connection is not established.")
                raise ConnectionError("Database connection is not established.")

            if not customer_nos:
                logger.error("Customer number list is empty.")
                raise ValueError("Customer number list is empty.")    
        

            print(customer_nos)
            query=f'''SELECT  DISTINCT ac.CUSTOMER_CLASS_CODE "Customer Class",
                        AC.status,
            to_char(HP.CREATION_DATE, 'DD-MM-YYYY') "Creation Date",
            HP.PARTY_TYPE "Customer Type",
            DECODE(hca.customer_type,
                'R', 'External',
                'I', 'Internal',
                hca.customer_type)            "Account Type",
            ac.CUSTOMER_NAME "Customer Name", 
            ac.CUSTOMER_NUMBER "Customer No", 
            hps.PARTY_SITE_NAME " Site Name", 
            hps.PARTY_SITE_NUMBER " Site Number", to_char(hcsu.site_use_id) site_use_id,hcas.CUST_ACCOUNT_ID,
            hcsu.STATUS "Status_S",
            HCSU.LOCATION "Location",
            substrb(look.meaning, 1, 8) "Site Use",
            HP.COUNTRY "Country",
            HP.ADDRESS1 "Address1",
            HP.ADDRESS2 "Address2",
            HP.ADDRESS3 "Address3",
            HP.ADDRESS4 "Address4",hp.PARTY_NUMBER,
            --HP.CITY "City",
            hl.CITY,hl.LOCATION_ID,
            HP.POSTAL_CODE "Postal Code",
            HP.STATE "State",
            --HP.PROVINCE "Province",
            hl.PROVINCE "Territory",
            HCA.attribute10 "Sub-Dist. Credit %",
            HCA.attribute3 "Self Collect Rebate Amount",
            hps.ATTRIBUTE1 "Transport Rebate",
            hcsu.ATTRIBUTE1 "Team Leader",
            hcsu.ATTRIBUTE2 "Trade Type",
            HCA.attribute2 "Sales Territory", --HCPA.CURRENCY_CODE "Currency",
            (SELECT RT.NAME FROM RA_TERMS_TL RT WHERE RT.TERM_ID = HCSU.PAYMENT_TERM_ID AND RT.LANGUAGE = 'US') "Terms",
            (SELECT RT.DESCRIPTION FROM RA_TERMS_TL RT WHERE RT.TERM_ID = HCSU.PAYMENT_TERM_ID AND RT.LANGUAGE = 'US') "Terms Description",
            (SELECT DISTINCT HCP.OVERALL_CREDIT_LIMIT FROM hz_cust_profile_amts hcp WHERE HCP.CUST_ACCOUNT_ID = AC.CUSTOMER_ID 
                AND HCP.SITE_USE_ID = HCSU.SITE_USE_ID AND hcp.site_use_id is not null AND HCP.CURRENCY_CODE ='KES' ) "KES Credit Limit",
            (SELECT DISTINCT HCP.OVERALL_CREDIT_LIMIT FROM hz_cust_profile_amts hcp WHERE HCP.CUST_ACCOUNT_ID = AC.CUSTOMER_ID 
                AND HCP.SITE_USE_ID = HCSU.SITE_USE_ID AND hcp.site_use_id is not null AND HCP.CURRENCY_CODE ='USD' ) "USD Credit Limit",
            (SELECT DISTINCT HCP.OVERALL_CREDIT_LIMIT FROM hz_cust_profile_amts hcp WHERE HCP.CUST_ACCOUNT_ID = AC.CUSTOMER_ID 
                AND HCP.SITE_USE_ID = HCSU.SITE_USE_ID AND hcp.site_use_id is not null AND HCP.CURRENCY_CODE = 'GBP' ) "GBP Credit Limit",
            PLH.PRICE_LIST "Pricelist Name",  hcsu.PRICE_LIST_ID, OET.TRANSACTION_TYPE_ID,hcsu.ORDER_TYPE_ID,
            OET.NAME "Order Type",
            DECODE(hcp.credit_checking, 
                'Y', 'Yes', 
                'N',  'No',
                hcp.credit_checking)   "Credit Check",hcp.CREDIT_HOLD,hcp.AUTOCASH_HIERARCHY_ID, hcp.AUTOCASH_HIERARCHY_ID_FOR_ADR,
            HCSU.FREIGHT_TERM "Freight Terms",
            (select col.Name from ar_collectors col
            where col.collector_id = hcp.collector_id) "Collector",
            srid.NAME "Default Sales Person",
    (select rid.RESOURCE_NAME from 
            JTF_RS_DEFRESOURCES_V rid where HCSU.primary_salesrep_id = srid.salesrep_id  
            and srid.RESOURCE_ID = rid.RESOURCE_ID) "Sales Person", --added by Godfrey 01/09/2016
            (select cpc.Name from hz_cust_profile_classes cpc 
            where hcp.profile_class_id=cpc.profile_class_id (+)) "Profile Class",
            AC.TAXPAYER_ID " TaxPayer ID",
            AC.TAX_REFERENCE "Tax Registration No",
            AC.ATTRIBUTE4 "Company Register ID",
    (SELECT
            bank.party_name                   bank_name
            FROM hz_parties               bank
            , hz_relationships         rel
            , hz_parties               branch
            , hz_organization_profiles bank_prof
            , hz_organization_profiles branch_prof
            , iby_ext_bank_accounts    account
            , iby_external_payers_all  ext_payer
            , iby_pmt_instr_uses_all   acc_instr
            , hz_parties               cust
            , hz_cust_accounts         cust_acct
            , ar_collectors           col
            , hz_cust_site_uses_all    cust_uses
            , hz_party_sites           party_site
            , hz_locations             cust_loc
                WHERE 1=1
                    AND bank.party_id                    = rel.object_id
                    and bank.party_type                  = rel.object_type
                    AND rel.object_table_name            = 'HZ_PARTIES'
                    AND rel.relationship_code            = 'BRANCH_OF'
                    AND rel.subject_id                   = branch.party_id
                    AND rel.subject_type                 = branch.party_type
                    AND rel.subject_table_name           = 'HZ_PARTIES'
                    AND bank.party_id                    = bank_prof.party_id
                    AND branch.party_id                  = branch_prof.party_id
                    AND cust_acct.cust_account_id        = HCAS.cust_account_id
                    AND HCAS.cust_acct_site_id      = cust_uses.cust_acct_site_id
                    AND party_site.party_id              = cust.party_id
                    AND party_site.party_site_id         = HCAS.party_site_id
                    AND party_site.location_id           = cust_loc.location_id
                    AND cust.party_id                    = cust_acct.party_id
                    AND bank.party_id                    = account.bank_id
                    AND branch.party_id                  = account.branch_id
                    AND account.ext_bank_account_id      = acc_instr.instrument_id
                    AND acc_instr.ext_pmt_party_id       = ext_payer.ext_payer_id
                    AND ext_payer.cust_account_id        = cust_acct.cust_account_id
                    AND cust_uses.site_use_id            = ext_payer.acct_site_use_id
                    AND ROWNUM=1) "Bank Name",
    (SELECT
            account.bank_account_num
            FROM hz_parties               bank
            , hz_relationships         rel
            , hz_parties               branch
            , hz_organization_profiles bank_prof
            , hz_organization_profiles branch_prof
            , iby_ext_bank_accounts    account
            , iby_external_payers_all  ext_payer
            , iby_pmt_instr_uses_all   acc_instr
            , hz_parties               cust
            , hz_cust_accounts         cust_acct
            , hz_cust_site_uses_all    cust_uses
            , hz_party_sites           party_site
            , hz_locations             cust_loc
    WHERE 1=1
                AND bank.party_id                    = rel.object_id
                and bank.party_type                  = rel.object_type
                AND rel.object_table_name            = 'HZ_PARTIES'
                AND rel.relationship_code            = 'BRANCH_OF'
                AND rel.subject_id                   = branch.party_id
                AND rel.subject_type                 = branch.party_type
                AND rel.subject_table_name           = 'HZ_PARTIES'
                AND bank.party_id                    = bank_prof.party_id
                AND branch.party_id                  = branch_prof.party_id
                AND cust_acct.cust_account_id        = HCAS.cust_account_id
                AND HCAS.cust_acct_site_id      = cust_uses.cust_acct_site_id
                AND party_site.party_id              = cust.party_id
                AND party_site.party_site_id         = HCAS.party_site_id
                AND party_site.location_id           = cust_loc.location_id
                AND cust.party_id                    = cust_acct.party_id
                AND bank.party_id                    = account.bank_id
                AND branch.party_id                  = account.branch_id
                AND account.ext_bank_account_id      = acc_instr.instrument_id
                AND acc_instr.ext_pmt_party_id       = ext_payer.ext_payer_id
                AND ext_payer.cust_account_id        = cust_acct.cust_account_id
                AND cust_uses.site_use_id            = ext_payer.acct_site_use_id
                AND ROWNUM=1) "Bank A/C Number",
    (SELECT
                branch.party_name                 branch_name
            FROM hz_parties               bank
            , hz_relationships         rel
            , hz_parties               branch
            , hz_organization_profiles bank_prof
            , hz_organization_profiles branch_prof
            , iby_ext_bank_accounts    account
            , iby_external_payers_all  ext_payer
            , iby_pmt_instr_uses_all   acc_instr
            , hz_parties               cust
            , hz_cust_accounts         cust_acct
            , hz_cust_site_uses_all    cust_uses
            , hz_party_sites           party_site
            , hz_locations             cust_loc
    WHERE 1=1
                AND bank.party_id                    = rel.object_id
                and bank.party_type                  = rel.object_type
                AND rel.object_table_name            = 'HZ_PARTIES'
                AND rel.relationship_code            = 'BRANCH_OF'
                AND rel.subject_id                   = branch.party_id
                AND rel.subject_type                 = branch.party_type
                AND rel.subject_table_name           = 'HZ_PARTIES'
                AND bank.party_id                    = bank_prof.party_id
                AND branch.party_id                  = branch_prof.party_id
                AND cust_acct.cust_account_id        = HCAS.cust_account_id
                AND HCAS.cust_acct_site_id      = cust_uses.cust_acct_site_id
                AND party_site.party_id              = cust.party_id
                AND party_site.party_site_id         = HCAS.party_site_id
                AND party_site.location_id           = cust_loc.location_id
                AND cust.party_id                    = cust_acct.party_id
                AND bank.party_id                    = account.bank_id
                AND branch.party_id                  = account.branch_id
                AND account.ext_bank_account_id      = acc_instr.instrument_id
                AND acc_instr.ext_pmt_party_id       = ext_payer.ext_payer_id
                AND ext_payer.cust_account_id        = cust_acct.cust_account_id
                AND cust_uses.site_use_id            = ext_payer.acct_site_use_id
                AND ROWNUM=1) "Bank Branch Name",
    (SELECT 
                ACV.FIRST_NAME||' '||ACV.LAST_NAME "Contact Person"
                FROM  
                AR_CONTACTS_V ACV,
                hz_parties h_contact ,
                hz_contact_points hcp,
                HZ_PARTIES HP,
                HZ_PARTIES HPP,
                HZ_CONTACT_POINTS HZP,
                HZ_ORG_CONTACTS HOC,
                HZ_RELATIONSHIPS HR,
                HZ_CUST_ACCOUNT_ROLES HCAR,
                HZ_CUST_ACCOUNTS HCA,
                HZ_PARTY_SITES HPS
            WHERE
                ACV.CONTACT_POINT_ID=HZP.CONTACT_POINT_ID
                and hr.subject_id = h_contact.PARTY_ID
                and hr.object_id = hp.party_id
                and hcp.owner_table_id(+) = hr.party_id
                and hca.party_id = hp.party_id
                and hcp.STATUS = 'A'
                AND HCA.CUST_ACCOUNT_ID = HCAS.CUST_ACCOUNT_ID 
                AND HCAS.CUST_ACCOUNT_ID = HCAR.CUST_ACCOUNT_ID
                AND HCAS.CUST_ACCT_SITE_ID = HCAR.CUST_ACCT_SITE_ID
                AND HP.PARTY_ID = HCA.PARTY_ID
                AND HPS.PARTY_ID = HCA.PARTY_ID
                AND HR.OBJECT_ID = HP.PARTY_ID
                AND HR.SUBJECT_ID = HPP.PARTY_ID
                AND HPS.PARTY_SITE_ID = HCAS.PARTY_SITE_ID
                AND HR.RELATIONSHIP_ID = HOC.PARTY_RELATIONSHIP_ID
                AND HZP.OWNER_TABLE_ID = HCAR.PARTY_ID
                AND HOC.ORG_CONTACT_ID=ACV.ORG_CONTACT_ID
                AND ROWNUM=1) "Contact Person",
    (
    SELECT 
                ACV.JOB_TITLE "Job Title" 
            FROM  
                AR_CONTACTS_V ACV,
                hz_parties h_contact ,
                hz_contact_points hcp,
                HZ_PARTIES HP,
                HZ_PARTIES HPP,
                HZ_CONTACT_POINTS HZP,
                HZ_ORG_CONTACTS HOC,
                HZ_RELATIONSHIPS HR,
                HZ_CUST_ACCOUNT_ROLES HCAR,
                HZ_CUST_ACCOUNTS HCA,
                HZ_PARTY_SITES HPS
        WHERE
                ACV.CONTACT_POINT_ID=HZP.CONTACT_POINT_ID
                and hr.subject_id = h_contact.PARTY_ID
                and hr.object_id = hp.party_id
                and hcp.owner_table_id(+) = hr.party_id
                and hca.party_id = hp.party_id
                and hcp.STATUS = 'A'
                AND HCA.CUST_ACCOUNT_ID = HCAS.CUST_ACCOUNT_ID 
                AND HCAS.CUST_ACCOUNT_ID = HCAR.CUST_ACCOUNT_ID
                AND HCAS.CUST_ACCT_SITE_ID = HCAR.CUST_ACCT_SITE_ID
                AND HP.PARTY_ID = HCA.PARTY_ID
                AND HPS.PARTY_ID = HCA.PARTY_ID
                AND HR.OBJECT_ID = HP.PARTY_ID
                AND HR.SUBJECT_ID = HPP.PARTY_ID
                AND HPS.PARTY_SITE_ID = HCAS.PARTY_SITE_ID
                AND HR.RELATIONSHIP_ID = HOC.PARTY_RELATIONSHIP_ID
                AND HZP.OWNER_TABLE_ID = HCAR.PARTY_ID
                AND HOC.ORG_CONTACT_ID=ACV.ORG_CONTACT_ID
                AND ROWNUM=1) "Job Title",
    (SELECT
            DISTINCT PHONE_COUNTRY_CODE||'-'||PHONE_AREA_CODE||'-'||PHONE_NUMBER
            FROM 
                HZ_PARTIES HP,
                HZ_PARTIES HPP,
                HZ_CONTACT_POINTS HZP,
                HZ_ORG_CONTACTS HOC,
                HZ_RELATIONSHIPS HR,
                HZ_CUST_ACCOUNT_ROLES HCAR,
                HZ_CUST_ACCOUNTS HCA,
                HZ_PARTY_SITES HPS
        WHERE 
                HCA.CUST_ACCOUNT_ID = HCAS.CUST_ACCOUNT_ID 
            AND HCAS.CUST_ACCOUNT_ID = HCAR.CUST_ACCOUNT_ID
            AND HCAS.CUST_ACCT_SITE_ID = HCAR.CUST_ACCT_SITE_ID
            AND HP.PARTY_ID = HCA.PARTY_ID
            AND HPS.PARTY_ID = HCA.PARTY_ID
            AND HR.OBJECT_ID = HP.PARTY_ID
            AND HR.SUBJECT_ID = HPP.PARTY_ID
            AND HPS.PARTY_SITE_ID = HCAS.PARTY_SITE_ID
            AND HR.RELATIONSHIP_ID = HOC.PARTY_RELATIONSHIP_ID
            AND HZP.OWNER_TABLE_ID = HCAR.PARTY_ID
            AND HP.PARTY_ID = HPSEL.PARTY_ID
            AND HP.PARTY_TYPE='ORGANIZATION'
            AND HZP.CONTACT_POINT_TYPE = 'PHONE'
            AND ROWNUM = 1) "Phone Number",
    (SELECT
            DISTINCT HZP.EMAIL_ADDRESS
        FROM 
            HZ_PARTIES HP,
            HZ_PARTIES HPP,
            HZ_CONTACT_POINTS HZP,
            HZ_ORG_CONTACTS HOC,
            HZ_RELATIONSHIPS HR,
            HZ_CUST_ACCOUNT_ROLES HCAR,
            HZ_CUST_ACCOUNTS HCA,
            HZ_PARTY_SITES HPS
    WHERE 
                HCA.CUST_ACCOUNT_ID = HCAS.CUST_ACCOUNT_ID 
            AND HCAS.CUST_ACCOUNT_ID = HCAR.CUST_ACCOUNT_ID
            AND HCAS.CUST_ACCT_SITE_ID = HCAR.CUST_ACCT_SITE_ID
            AND HP.PARTY_ID = HCA.PARTY_ID
            AND HPS.PARTY_ID = HCA.PARTY_ID
            AND HR.OBJECT_ID = HP.PARTY_ID
            AND HR.SUBJECT_ID = HPP.PARTY_ID
            AND HPS.PARTY_SITE_ID = HCAS.PARTY_SITE_ID
            AND HR.RELATIONSHIP_ID = HOC.PARTY_RELATIONSHIP_ID
            AND HZP.OWNER_TABLE_ID = HCAR.PARTY_ID
            AND HP.PARTY_TYPE='ORGANIZATION'
            AND HZP.CONTACT_POINT_TYPE = 'EMAIL'
            AND HP.PARTY_ID = HPSEL.PARTY_ID
            AND ROWNUM=1
            ) "Email Address",
        hp.PARTY_ID,hp.EMAIL_ADDRESS 
    --=============================================================================
    --Tables
    -------------------------------------------------------------------------------
    FROM 
            AR_CUSTOMERS AC,
            ra_salesreps_all        srid,
            HZ_CUST_ACCT_SITES_ALL HCAS, 
            hz_cust_accounts HCA, 
            HZ_CUST_SITE_USES_ALL HCSU, 
            HZ_PARTY_SITES HPS,
            hz_parties HP,
            HZ_LOCATIONS hl,
            hz_parties HPSEL,
            ar_lookups look,
            ar_lookups look_status,
            OE_Price_Lists_Active_V PLH,
            OE_TRANSACTION_TYPES_TL OET, --hz_cust_profile_amts hcpa,
            hz_customer_profiles    hcp
            
    --=============================================================================
    WHERE   AC.CUSTOMER_ID = HCAS.CUST_ACCOUNT_ID
            AND HCAS.CUST_ACCT_SITE_ID = HCSU.CUST_ACCT_SITE_ID
            AND HCAS.PARTY_SITE_ID = HPS.PARTY_SITE_ID
            AND HCA.CUST_ACCOUNT_ID = AC.CUSTOMER_ID
            AND hca.cust_account_id    =  hcp.cust_account_id
            AND HCA.PARTY_ID = HP.PARTY_ID
            AND look.lookup_type(+) = 'SITE_USE_CODE'
            AND look.lookup_code(+) = HCSU.SITE_USE_CODE
            AND look_status.lookup_type(+) = 'CODE_STATUS'
            AND look_status.lookup_code(+) = nvl(AC.status, 'A')
            AND HCSU.PRICE_LIST_ID =PLH.PRICE_LIST_ID(+)
            AND HCSU.primary_salesrep_id         = srid.salesrep_id(+)
            and HCSU.ORDER_TYPE_ID = OET.TRANSACTION_TYPE_ID (+)
            AND HP.PARTY_ID = HPSEL.PARTY_ID 
            AND hps.LOCATION_ID = hl.LOCATION_ID
            --and   HCPA.CUST_ACCOUNT_ID = AC.CUSTOMER_ID
            --AND HCPA.CURRENCY_CODE is not null 
    --        AND HCPA.SITE_USE_ID --= HCSU.SITE_USE_ID 
    --        in ('8601',
    --'8600',
    --'453574',
    --'453574',
    --'1242',
    --'1241',
    --'1243',
    --'1240',
    --'15065',
    --'597591',
    --'597589',
    --'517583',
    --'364567',
    --'1247',
    --'623589',
    --'580578',
    --'580578',
    --'520571',
    --'453568',
    --'453568',
    --'274557',
    --'546573',
    --'5819',
    --'4034',
    --'8621',
    --'8620',
    --'453571',
    --'6215',
    --'13921',
    --'1276',
    --'12260',
    --'8384',
    --'1280',
    --'1622',
    --'1621',
    --'1622',
    --'1621',
    --'19355',
    --'19352',
    --'425576',
    --'6414',
    --'9200',
    --'9200',
    --'287553',
    --'287555',
    --'1184',
    --'8485',
    --'5922',
    --'246549') 
        --AND HP.CREATION_DATE BETWEEN NVL (:p_from_date, HP.CREATION_DATE)
                                    --AND NVL (:p_to_date, HP.CREATION_DATE)
            --AND AC.CUSTOMER_NAME=:C_Name
            --AND HPS.PARTY_SITE_NAME=:SITE_NAME
            and AC.status = 'A'
            and hcsu.STATUS = 'A'
            AND AC.CUSTOMER_NUMBER IN ({customer_no})'''
            df = self.cursor.execute(query)
            rows = df.fetchall()
            print("Sucessfully fetched data")
            columns = [desc[0] for desc in self.cursor.description]  # Get column names

            
            data = pd.DataFrame(rows, columns=columns)

            logger.info(f"fetched customer data of shape {data.shape}")
            data['customerKey']=data.apply(
                   lambda x : self.normalize(x['Location'])+'_'+str(x['Customer No']),axis=1
            )
            
        
            
            data=data[['Customer No','Customer Name','EMAIL_ADDRESS','Phone Number','Customer Type','Terms','Location','customerKey']]
            print(data.columns)
            data = data.rename(columns={
                                            'Customer No': 'customerNumber',
                                            'Customer Name': 'name',
                                            'EMAIL_ADDRESS': 'email',
                                            'Phone Number': 'phone',
                                            'Customer Type': 'customerType',
                                            'Terms': 'creditTerms',
                                            'Location':'location'
                                        })
            print(data.head())
            data=data.drop_duplicates()
            cleaned=data.groupby(['customerKey']).agg({'name': 'first',
                                            'email': 'first',
                                            'phone': 'first',
                                            'customerType': 'first',
                                            'creditTerms': 'first',
                                            'customerNumber':'first',
                                            'location':'first'}).reset_index()
            logger.info(f"sucessfully returned data data columns are {cleaned.columns} with shape {cleaned.shape}")
            print(cleaned.columns)
            return cleaned
        

        except Exception as e:
            logger.exception(f" Failed to fetch customer data: {e}")
            raise RuntimeError(f"Failed to fetch customer data: {e}")

      


# class DataFetcherOracleCustomer:




#     def normalize(self,text):
#         if pd.isna(text):
#             return ""
#         text = text.strip().lower()
#         text = re.sub(r'[^a-z0-9]', '', text)  # remove all non-alphanumeric characters
#         return text

   



#     def fetch_invoice_data(self,date):

#         data=pd.read_excel('storage/Invoicedata_master_data.xls')
#         data['customerKey']=data.apply(
#             lambda x:self.normalize(x['SHIP_TO_LOCATIONS1'])+'_'+str(x['Customer No']),axis=1
#                                                         )
#         final_data=data[['INVOICE_NUMBER','INVOICE_DATE','Customer No','INVOICE_GROSS_VALUE','INVOICE_CURRENCY_CODE','customerKey']]
#         final_data=final_data[final_data['INVOICE_DATE']>date]
        
#         cleaned=final_data.groupby(['INVOICE_NUMBER']).agg({
#                                                         'INVOICE_DATE': 'first',
#                                                         'Customer No': 'first',
#                                                         'INVOICE_GROSS_VALUE': 'sum',
#                                                         'INVOICE_CURRENCY_CODE': 'first',
#                                                         'customerKey':'first'

#                                                         }).reset_index()
#         cleaned=cleaned.rename(columns={
#                                             'INVOICE_NUMBER': 'invoiceNumber',
#                                             'INVOICE_DATE': 'invoiceDate',
#                                             'Customer No': 'customerNumber',
#                                             'INVOICE_GROSS_VALUE': 'invoiceGrossValue',
#                                             'INVOICE_CURRENCY_CODE':'invoiceCurrencyCode'
#                                         })
#         return cleaned

        


        


#     def fetch_customer_data(self,customer_nos:list):
#         data=pd.read_excel('storage/Customer_master_data.xls')
#         data['customerKey']=data.apply(
#     lambda x : self.normalize(x['Location'])+'_'+str(x['Customer No']),axis=1
# )
        
#         data=data[['Customer No','Customer Name','EMAIL_ADDRESS','Phone Number','Customer Type','Terms','Location','customerKey']]
#         print(data.columns)
#         data = data.rename(columns={
#                                         'Customer No': 'customerNumber',
#                                         'Customer Name': 'name',
#                                         'EMAIL_ADDRESS': 'email',
#                                         'Phone Number': 'phone',
#                                         'Customer Type': 'customerType',
#                                         'Terms': 'creditTerms',
#                                         'Location':'location'
#                                     })
#         cleaned=data[data['customerNumber'].isin(customer_nos)]
#         cleaned=cleaned.groupby(['customerKey']).agg({'name': 'first',
#                                         'email': 'first',
#                                         'phone': 'first',
#                                         'customerType': 'first',
#                                         'creditTerms': 'max',
#                                         'customerNumber':'mean',
#                                         'location':'first'}).reset_index()
#         print(cleaned.columns)
#         return cleaned
