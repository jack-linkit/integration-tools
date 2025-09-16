"""
Database management utilities with connection handling and query helpers.
"""

import urllib.parse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple
import pytz

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session, sessionmaker

from integration_tools.models import DataRequestType, Request, RequestEmailNotification, XpsDistrictUpload


def convert_utc_to_est(utc_dt: Optional[datetime]) -> Optional[datetime]:
    """
    Convert UTC datetime to Eastern Time (EST/EDT).
    
    Args:
        utc_dt: UTC datetime to convert
        
    Returns:
        Datetime converted to Eastern timezone or None if input is None
    """
    if utc_dt is None:
        return None
    
    # Ensure the datetime is timezone-aware (assume UTC if naive)
    if utc_dt.tzinfo is None:
        utc_dt = pytz.UTC.localize(utc_dt)
    elif utc_dt.tzinfo != pytz.UTC:
        # Convert to UTC first if it's in a different timezone
        utc_dt = utc_dt.astimezone(pytz.UTC)
    
    # Convert to Eastern timezone (handles EST/EDT automatically)
    eastern = pytz.timezone('US/Eastern')
    return utc_dt.astimezone(eastern)


@dataclass
class RequestRow:
    """Data class for request query results."""
    RequestID: int
    DistrictID: int
    DataRequestTypeID: int
    DataRequestTypeName: str
    ImportedFileName: Optional[str]
    Status: Optional[int]
    RequestTime: Optional[datetime]
    RequestTimeEST: Optional[datetime] = None


class DatabaseManager:
    """Manages database connections and provides query utilities."""
    
    def __init__(self, server: str = "10.252.24.190", database: str = "AdminReporting"):
        self.server = server
        self.database = database
        self._engine = None
        self._session_factory = None
    
    def _get_connection_string(self, username: str, password: str) -> str:
        """Build SQL Server connection string."""
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        return f"mssql+pyodbc:///?odbc_connect={params}"
    
    @contextmanager
    def get_session(self, username: str, password: str):
        """
        Get a database session with provided credentials.
        
        Args:
            username: Database username
            password: Database password
            
        Yields:
            Database session
        """
        conn_str = self._get_connection_string(username, password)
        engine = create_engine(conn_str)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def list_request_types(
        self, 
        session: Session, 
        name_filter: Optional[str] = None
    ) -> List[Tuple[int, Optional[str]]]:
        """
        List all DataRequestTypes with optional name filtering.
        
        Args:
            session: Database session
            name_filter: Optional name filter (case-insensitive substring match)
            
        Returns:
            List of (DataRequestTypeID, Name) tuples
        """
        query = session.query(DataRequestType.DataRequestTypeID, DataRequestType.Name)
        if name_filter:
            query = query.filter(DataRequestType.Name.ilike(f"%{name_filter}%"))
        rows = query.order_by(DataRequestType.Name).all()
        return [(int(tid), name) for (tid, name) in rows]

    def find_latest_requests(
        self,
        session: Session,
        type_ids: Optional[List[int]] = None,
        type_name_prefixes: Optional[List[str]] = None,
        district_ids: Optional[List[int]] = None,
        statuses: Sequence[int] = (4, 5),
        since_date: Optional[datetime] = None,
    ) -> List[RequestRow]:
        """
        Find latest requests by various criteria.
        
        Args:
            session: Database session
            type_ids: Optional list of DataRequestTypeIDs
            type_name_prefixes: Optional list of type name prefixes
            district_ids: Optional list of DistrictIDs
            statuses: Request statuses to include (default: 4=failed, 5=success)
            since_date: Optional datetime to filter requests since this date
            
        Returns:
            List of RequestRow objects
        """
        # Resolve effective type IDs list first
        effective_type_ids: List[int] = []
        if type_ids:
            effective_type_ids.extend(type_ids)
            
        if type_name_prefixes:
            from sqlalchemy import or_
            clauses = [DataRequestType.Name.ilike(f"{p}%") for p in type_name_prefixes]
            trows = (
                session.query(DataRequestType.DataRequestTypeID)
                .filter(or_(*clauses))
                .all()
            )
            effective_type_ids.extend([int(r[0]) for r in trows])

        effective_type_ids = sorted(set(effective_type_ids))

        # Get latest request per (DistrictID, DataRequestTypeID)
        latest_query = (
            session.query(
                Request.DistrictID,
                Request.DataRequestTypeID,
                func.max(Request.RequestID).label("MaxRequestID"),
            )
            .filter(Request.Status.in_(list(statuses)))
            .group_by(Request.DistrictID, Request.DataRequestTypeID)
        )
        
        # Apply filters early for better performance
        if district_ids:
            latest_query = latest_query.filter(Request.DistrictID.in_(district_ids))
            
        if effective_type_ids:
            latest_query = latest_query.filter(Request.DataRequestTypeID.in_(effective_type_ids))
            
        latest = latest_query.subquery()

        query = (
            session.query(
                Request.RequestID,
                Request.DistrictID,
                Request.DataRequestTypeID,
                DataRequestType.Name.label("DataRequestTypeName"),
                Request.ImportedFileName,
                Request.Status,
                Request.RequestTime,
            )
            .join(
                latest,
                (Request.DistrictID == latest.c.DistrictID)
                & (Request.DataRequestTypeID == latest.c.DataRequestTypeID)
                & (Request.RequestID == latest.c.MaxRequestID),
            )
            .join(DataRequestType, DataRequestType.DataRequestTypeID == Request.DataRequestTypeID)
        )

        if since_date:
            query = query.filter(Request.RequestTime >= since_date)

        rows = query.order_by(Request.RequestTime.desc()).all()
        return [
            RequestRow(
                RequestID=r.RequestID,
                DistrictID=r.DistrictID,
                DataRequestTypeID=r.DataRequestTypeID,
                DataRequestTypeName=r.DataRequestTypeName,
                ImportedFileName=r.ImportedFileName,
                Status=r.Status,
                RequestTime=r.RequestTime,
                RequestTimeEST=convert_utc_to_est(r.RequestTime),
            )
            for r in rows
        ]

    def get_email_content(
        self, 
        session: Session, 
        request_id: int
    ) -> Optional[Tuple[str, Optional[str]]]:
        """
        Get email content for a request.
        
        Args:
            session: Database session
            request_id: Request ID
            
        Returns:
            Tuple of (email_content, file_attach_content) or None if not found
        """
        rec = (
            session.query(
                RequestEmailNotification.EmailContent,
                RequestEmailNotification.FileAttachContent
            )
            .filter(RequestEmailNotification.RequestID == request_id)
            .first()
        )
        if rec is None:
            return None
        return rec.EmailContent, rec.FileAttachContent

    def get_directory_path_for_request(
        self, 
        session: Session, 
        request_id: int
    ) -> Optional[str]:
        """
        Get directory path for a request.
        
        Args:
            session: Database session
            request_id: Request ID
            
        Returns:
            Directory path or None if not found
        """
        r: Optional[Request] = session.query(Request).filter(Request.RequestID == request_id).first()
        if not r:
            return None
        if r.ImportedFileName:
            return r.ImportedFileName
            
        du = (
            session.query(XpsDistrictUpload.DirectoryPath)
            .filter(XpsDistrictUpload.DistrictID == r.DistrictID)
            .first()
        )
        return du.DirectoryPath if du else None

    def clear_checksums(
        self,
        session: Session,
        district_ids: Sequence[int],
        directory_paths: Optional[Sequence[str]] = None,
        keys: Optional[Sequence[str]] = None,
    ) -> int:
        """
        Delete UploadFileIntegrationChecksum rows for schedules.
        
        Args:
            session: Database session
            district_ids: District IDs to target
            directory_paths: Optional directory paths to filter by
            keys: Optional checksum keys to filter by
            
        Returns:
            Number of rows deleted
        """
        def make_in_clause(col: str, values: Sequence, param_name_prefix: str) -> Tuple[str, Dict[str, object]]:
            names = []
            params: Dict[str, object] = {}
            for idx, val in enumerate(values):
                name = f"{param_name_prefix}{idx}"
                names.append(f":{name}")
                params[name] = val
            return f"{col} IN (" + ",".join(names) + ")", params

        clauses = ["du.UploadTypeID = 5", "du.ClassNameType = 2", "du.Run = 1"]
        params: Dict[str, object] = {}

        district_clause, p = make_in_clause("du.DistrictID", sorted(set(district_ids)), "d_")
        clauses.append(district_clause)
        params.update(p)

        if directory_paths:
            dir_clause, p = make_in_clause("du.DirectoryPath", sorted(set(directory_paths)), "dir_")
            clauses.append(dir_clause)
            params.update(p)

        if keys:
            key_clause, p = make_in_clause("ufc.[key]", sorted(set(keys)), "k_")
            clauses.append(key_clause)
            params.update(p)

        sql = f"""
            DELETE ufc
            FROM UploadFileIntegrationChecksum ufc
            JOIN dbo.xpsDistrictUpload du ON ufc.XpsDistrictUploadID = du.xpsDistrictUploadID
            WHERE {' AND '.join(clauses)}
        """
        res = session.execute(text(sql), params)
        session.commit()
        rows_affected = int((getattr(res, "rowcount", 0) or 0))
        return rows_affected

    def bump_latest_queue(
        self,
        session: Session,
        district_ids: Sequence[int],
        directory_paths: Optional[Sequence[str]] = None,
    ) -> int:
        """
        Set latest xpsQueue row to pending/success state to trigger rerun.
        
        Args:
            session: Database session
            district_ids: District IDs to target
            directory_paths: Optional directory paths to filter by
            
        Returns:
            Number of rows updated
        """
        def make_in_clause(col: str, values: Sequence, param_name_prefix: str) -> Tuple[str, Dict[str, object]]:
            names = []
            params: Dict[str, object] = {}
            for idx, val in enumerate(values):
                name = f"{param_name_prefix}{idx}"
                names.append(f":{name}")
                params[name] = val
            return f"{col} IN (" + ",".join(names) + ")", params

        clauses = ["du.UploadTypeID = 5", "du.ClassNameType = 2", "du.Run = 1"]
        params: Dict[str, object] = {}
        district_clause, p = make_in_clause("du.DistrictID", sorted(set(district_ids)), "d_")
        clauses.append(district_clause)
        params.update(p)
        
        if directory_paths:
            dir_clause, p = make_in_clause("du.DirectoryPath", sorted(set(directory_paths)), "dir_")
            clauses.append(dir_clause)
            params.update(p)

        # Update only the latest queue row per DistrictID
        sql = f"""
            UPDATE q
            SET q.xpsQueueStatusID = 2, q.xpsQueueResultID = 5
            FROM dbo.xpsQueue q
            JOIN (
                SELECT du.DistrictID, MAX(q2.xpsQueueID) AS MaxQ
                FROM dbo.xpsQueue q2
                JOIN dbo.xpsDistrictUpload du ON q2.xpsDistrictUploadID = du.xpsDistrictUploadID
                WHERE {' AND '.join(clauses)}
                GROUP BY du.DistrictID
            ) latest ON latest.MaxQ = q.xpsQueueID
        """
        res = session.execute(text(sql), params)
        session.commit()
        rows_affected = int((getattr(res, "rowcount", 0) or 0))
        return rows_affected