from sqlmodel import SQLModel, Field
from datetime import datetime,date
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional


class SupplierPayment(SQLModel, table=True):

    __tablename__ = "supplierPayment"

    supplierPaymentId: int = Field(default=None, primary_key=True)

    invoiceUniqueKey:str 
    invoiceDate: date

    vendorId: str= Field(nullable=True)



    invoiceNumber: str = Field(max_length=50,nullable=True)

    supplierId: int = Field(foreign_key="suppliers.supplierId")


    creditAmount: float = Field(default=0.0,nullable=True)
    discountRate: float = Field(default=0.0)

    discountPercentage :float 
    
    originalPaymentDate: date
    offeredPaymentDate: date
    offerStatus: str= Field(default = "not_sent")




    offerSentBy: str = Field(max_length=100,nullable=True)
    offerSentDate: datetime = Field(nullable=True)
    offerDecisionDate: Optional[datetime]=Field(nullable=True)

    createAt: datetime= Field(default=datetime.now(),nullable=True)
    modifiedAt: datetime =Field(nullable=True)
    createdBy: str = Field(max_length=100,nullable= True)
    modifiedBy: str = Field(max_length=100, nullable=True)    


