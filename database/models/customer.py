from sqlmodel import SQLModel, Field
from datetime import datetime,date
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional

class Customer(SQLModel, table=True):



    __tablename__ = "customers"

    customerId: int = Field(default=None, primary_key=True)
    customerNumber: int = Field(default=None)
    customerKey:str =Field(default=None,nullable=False)
    name: str = Field(max_length=255,nullable=True)
    email: str = Field(max_length=255,nullable=True)
    phone: str = Field(max_length=50,nullable=True)
    customerType: str = Field(max_length=100,nullable=True)
    creditTerms: str=Field(nullable=False)

    createAt: datetime=Field(default=None,nullable=True)
    modifiedAt: datetime=Field(default=None,nullable=True)
    createdBy: str = Field(max_length=100,nullable=True)
    modifiedBy: str = Field(max_length=100,nullable=True)







