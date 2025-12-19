"""Query execution utilities for different database types."""

import sqlite3
import psycopg2
import mysql.connector
from pathlib import Path
from typing import List, Tuple
from .query_converters.base import QueryResult


class QueryExecutor:
    """Execute queries on different database types."""
    
    @staticmethod
    def execute_sqlite(db_path: str, query: str) -> QueryResult:
        """Execute query on SQLite database.
        
        Args:
            db_path: Path to SQLite database file
            query: SQL query to execute
            
        Returns:
            QueryResult with execution results
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            total_rows = len(rows)
            
            conn.close()
            
            return QueryResult(
                columns=columns,
                rows=rows,
                total_rows=total_rows
            )
        except Exception as e:
            return QueryResult(
                columns=[],
                rows=[],
                total_rows=0,
                error=str(e)
            )
    
    @staticmethod
    def execute_postgresql(
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        query: str
    ) -> QueryResult:
        """Execute query on PostgreSQL database.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            database: PostgreSQL database name
            query: SQL query to execute
            
        Returns:
            QueryResult with execution results
        """
        try:
            # Connect to PostgreSQL using psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            cursor = conn.cursor()
            cursor.execute(query)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            total_rows = len(rows)
            
            cursor.close()
            conn.close()
            
            return QueryResult(
                columns=columns,
                rows=rows,
                total_rows=total_rows
            )
        except Exception as e:
            return QueryResult(
                columns=[],
                rows=[],
                total_rows=0,
                error=str(e)
            )
    
    @staticmethod
    def execute_mysql(
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        query: str
    ) -> QueryResult:
        """Execute query on MySQL database.
        
        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
            database: MySQL database name
            query: SQL query to execute
            
        Returns:
            QueryResult with execution results
        """
        try:
            # Connect to MySQL using mysql.connector
            conn = mysql.connector.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=database
            )
            cursor = conn.cursor()
            cursor.execute(query)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            total_rows = len(rows)
            
            cursor.close()
            conn.close()
            
            return QueryResult(
                columns=columns,
                rows=rows,
                total_rows=total_rows
            )
        except Exception as e:
            return QueryResult(
                columns=[],
                rows=[],
                total_rows=0,
                error=str(e)
            )

