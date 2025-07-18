from sqlmodel import SQLModel, Field
from datetime import datetime,date
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional


class CustomerOffer(SQLModel, table=True):

    __tablename__ = "customerOffers"

    customerOfferId: int = Field(default=None, primary_key=True)

    invoiceDate: date
    invoiceNumber: str = Field(max_length=50,nullable=True)
    
    customerId: int = Field(foreign_key="customers.customerId")
    customerKey:str =Field(default=None,nullable=False)
    
    creditAmount: float = Field(default=0.0,nullable=True)
    discountRate: float = Field(default=0.0,)
    discountPercentage:float
    
    originalPaymentDate: date
    offeredPaymentDate: date
    totalInterest: float = Field(default=0.0, )
    
    offerSentBy: str = Field(max_length=100,nullable=True)
    offerSentDate: datetime = Field(nullable=True)
    offerDecisionDate: Optional[datetime]=Field(nullable=True)
    offerStatus: str= Field(default = "not_sent")

    createAt: datetime= Field(default=datetime.now(),nullable=True)
    modifiedAt: datetime =Field(nullable=True)
    createdBy: str = Field(max_length=100,nullable= True)
    modifiedBy: str = Field(max_length=100, nullable=True)    


