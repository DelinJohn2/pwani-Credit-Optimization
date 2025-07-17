from sqlmodel import SQLModel, Field
from datetime import datetime,date
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional

class Suppliers(SQLModel, table=True):



    __tablename__ = "suppliers"

    supplierId: int = Field(default=None, primary_key=True)
    vendorId: int = Field(default=None)
    
    name: str = Field(max_length=255,nullable=True)
    email: str = Field(max_length=255,nullable=True)
    phone: str = Field(max_length=50,nullable=True)
    supplierType: str = Field(max_length=100,nullable=True)
    creditTerms: str=Field(nullable=True)

    createAt: datetime=Field(default=None,nullable=True)
    modifiedAt: datetime=Field(default=None,nullable=True)
    createdBy: str = Field(max_length=100,nullable=True)
    modifiedBy: str = Field(max_length=100,nullable=True)





