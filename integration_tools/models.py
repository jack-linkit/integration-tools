"""
Database models for integration tools.
"""

from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List


class Base(DeclarativeBase):
    pass


class School(Base):
    __tablename__ = 'School'
    __table_args__ = {'schema': 'dbo'}

    SchoolID: Mapped[int] = mapped_column(primary_key=True)
    DistrictID: Mapped[int]
    Name: Mapped[Optional[str]]
    Code: Mapped[Optional[str]]
    TempName: Mapped[Optional[str]]
    StateCode: Mapped[Optional[str]]
    SISID: Mapped[Optional[str]]
    CreatedDate: Mapped[Optional[datetime]]
    ModifiedDate: Mapped[Optional[datetime]]
    ModifiedUser: Mapped[Optional[str]]
    ModifiedBy: Mapped[Optional[str]]
    Status: Mapped[Optional[int]]
    StateID: Mapped[Optional[int]]
    LocationCode: Mapped[Optional[str]]


class DataRequestType(Base):
    __tablename__ = 'DataRequestType'
    __table_args__ = {'schema': 'dbo'}

    DataRequestTypeID: Mapped[int] = mapped_column(primary_key=True)
    Name: Mapped[Optional[str]]
    requests: Mapped[List["Request"]] = relationship(
        back_populates="data_request_type"
    )


class Request(Base):
    __tablename__ = 'Request'
    __table_args__ = {'schema': 'dbo'}

    RequestID: Mapped[int] = mapped_column(primary_key=True)
    UserID: Mapped[int]
    DataRequestTypeID: Mapped[int] = mapped_column(
        ForeignKey('dbo.DataRequestType.DataRequestTypeID')
    )
    RequestTime: Mapped[Optional[datetime]]
    Email: Mapped[Optional[str]]
    DistrictID: Mapped[int]
    ImportedFileName: Mapped[Optional[str]]
    FileSize: Mapped[Optional[int]]
    FileRow: Mapped[Optional[int]]
    ImportedStartDate: Mapped[Optional[datetime]]
    ImportedEndDate: Mapped[Optional[datetime]]
    ImportDuration: Mapped[Optional[int]]
    TemplateID: Mapped[Optional[int]]
    Mode: Mapped[Optional[int]]
    Status: Mapped[Optional[int]]
    RequestType: Mapped[Optional[str]]
    UserIDs: Mapped[Optional[str]]
    DistrictIDs: Mapped[Optional[str]]
    IsDeleted: Mapped[int]
    HasBeenMoved: Mapped[int]

    data_request_type: Mapped["DataRequestType"] = relationship(
        back_populates="requests"
    )
    email_notification: Mapped[Optional["RequestEmailNotification"]] = relationship(
        back_populates="request", uselist=False
    )


class RequestEmailNotification(Base):
    __tablename__ = 'RequestEmailNotification'
    __table_args__ = {'schema': 'dbo'}

    ID: Mapped[int] = mapped_column(primary_key=True)
    RequestID: Mapped[int] = mapped_column(ForeignKey('dbo.Request.RequestID'))
    EmailContent: Mapped[str]
    FileAttachContent: Mapped[Optional[str]]
    request: Mapped["Request"] = relationship(back_populates="email_notification")


class DistrictDataParm(Base):
    __tablename__ = "DistrictDataParm"
    __table_args__ = {"schema": "dbo"}

    DistrictDataParmID: Mapped[int] = mapped_column(primary_key=True)
    DistrictID: Mapped[int]
    DataSetOriginID: Mapped[int]
    DataSetCategoryID: Mapped[int]
    JSONDataConfig: Mapped[Optional[str]]
    ImportType: Mapped[Optional[str]]


class XpsDistrictUpload(Base):
    __tablename__ = "xpsDistrictUpload"
    __table_args__ = {"schema": "dbo"}

    xpsDistrictUploadID: Mapped[int] = mapped_column(primary_key=True)
    DistrictID: Mapped[int]
    LastProcessed: Mapped[Optional[datetime]]
    DirectoryPath: Mapped[Optional[str]]
    ScheduledTime: Mapped[Optional[str]]
    Run: Mapped[Optional[int]]
    UploadTypeID: Mapped[Optional[int]]
    ClassNameType: Mapped[Optional[int]]
    SchoolID: Mapped[Optional[int]]
    GetExternalFiles: Mapped[Optional[str]]
    ExternalSourceInfo: Mapped[Optional[str]]


class DistrictTerm(Base):
    __tablename__ = "DistrictTerm"
    __table_args__ = {"schema": "dbo"}

    DistrictTermID: Mapped[int] = mapped_column(primary_key=True)
    Name: Mapped[Optional[str]]
    DistrictID: Mapped[int]
    DateStart: Mapped[Optional[datetime]]
    DateEnd: Mapped[Optional[datetime]]
    Code: Mapped[Optional[str]]
    CreatedByUserID: Mapped[Optional[int]]
    UpdatedByUserID: Mapped[Optional[int]]
    DateCreated: Mapped[Optional[datetime]]
    DateUpdated: Mapped[Optional[datetime]]
    SISID: Mapped[Optional[str]]
    ModifiedUser: Mapped[Optional[int]]
    ModifiedBy: Mapped[Optional[int]]


class XORDistrictTermMapping(Base):
    __tablename__ = "XORDistrictTermMapping"
    __table_args__ = {"schema": "dbo"}

    XORDistrictTermMappingID: Mapped[int] = mapped_column(primary_key=True)
    TermSourceID: Mapped[str]
    DistrictTermID: Mapped[int]
    DistrictTermName: Mapped[Optional[str]]
    DistrictID: Mapped[int]