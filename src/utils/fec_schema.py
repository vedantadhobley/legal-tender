"""FEC Schema Loader - Read field definitions directly from official FEC header files.

This module loads CSV headers from ~/workspace/.legal-tender/data/headers/*.csv 
(downloaded directly from FEC website) and provides them as the authoritative schema 
for parsing. This ensures zero drift between FEC's official format and our parsing logic.
"""

from pathlib import Path
from typing import List, Dict, Any
import csv

from src.utils.storage import get_data_dir


# Map download file names to their header file names
HEADER_FILE_MAP = {
    'cn': 'cn.csv',
    'cm': 'cm.csv',
    'ccl': 'ccl.csv',
    'pas2': 'pas2.csv',
    'oth': 'oth.csv',
    'indiv': 'indiv.csv',
}


class FECSchema:
    """Load and cache FEC schemas from official header files."""
    
    def __init__(self, headers_dir: Path | None = None):
        """Initialize schema loader.
        
        Args:
            headers_dir: Path to directory containing FEC header CSV files.
                        Defaults to ~/workspace/.legal-tender/data/headers/
        """
        if headers_dir is None:
            # Use storage location for headers
            headers_dir = get_data_dir() / 'headers'
        
        self.headers_dir = headers_dir
        self._cache: Dict[str, List[str]] = {}
    
    def get_fields(self, file_type: str) -> List[str]:
        """Get field names for a given FEC file type.
        
        Args:
            file_type: One of: 'cn', 'cm', 'ccl', 'pas2', 'oth', 'indiv'
        
        Returns:
            List of field names in order (e.g., ['CMTE_ID', 'AMNDT_IND', ...])
        
        Raises:
            ValueError: If file_type is unknown
            FileNotFoundError: If header file doesn't exist
        """
        if file_type not in HEADER_FILE_MAP:
            raise ValueError(
                f"Unknown file type '{file_type}'. "
                f"Valid types: {', '.join(HEADER_FILE_MAP.keys())}"
            )
        
        # Return cached if available
        if file_type in self._cache:
            return self._cache[file_type]
        
        # Load from CSV file
        header_file = self.headers_dir / HEADER_FILE_MAP[file_type]
        if not header_file.exists():
            raise FileNotFoundError(
                f"FEC header file not found: {header_file}\n"
                f"Expected official FEC header CSV at this location."
            )
        
        with open(header_file, 'r') as f:
            reader = csv.reader(f)
            fields = next(reader)  # First row contains field names
        
        # Cache and return
        self._cache[file_type] = fields
        return fields
    
    def parse_line(self, file_type: str, line: str, delimiter: str = '|') -> Dict[str, Any]:
        """Parse a delimited line into a dict using official field names.
        
        Args:
            file_type: One of: 'cn', 'cm', 'ccl', 'pas2', 'oth', 'indiv'
            line: Pipe-delimited line from FEC bulk data file
            delimiter: Field delimiter (default: '|')
        
        Returns:
            Dict mapping field names to values (e.g., {'CMTE_ID': 'C00401224', ...})
            
        Example:
            >>> schema = FECSchema()
            >>> record = schema.parse_line('cm', 'C00401224|ACTBLUE|...')
            >>> record['CMTE_ID']
            'C00401224'
        """
        fields = self.get_fields(file_type)
        values = line.strip().split(delimiter)
        
        # Pad with empty strings if line has fewer fields than expected
        if len(values) < len(fields):
            values.extend([''] * (len(fields) - len(values)))
        
        return dict(zip(fields, values))
    
    def get_field_count(self, file_type: str) -> int:
        """Get the number of fields for a file type.
        
        Args:
            file_type: One of: 'cn', 'cm', 'ccl', 'pas2', 'oth', 'indiv'
        
        Returns:
            Number of fields (e.g., 22 for pas2, 21 for oth/indiv)
        """
        return len(self.get_fields(file_type))


# Singleton instance for convenience
_default_schema = None


def get_schema() -> FECSchema:
    """Get the default FEC schema loader instance."""
    global _default_schema
    if _default_schema is None:
        _default_schema = FECSchema()
    return _default_schema


def get_fields(file_type: str) -> List[str]:
    """Convenience function to get fields using default schema.
    
    Args:
        file_type: One of: 'cn', 'cm', 'ccl', 'pas2', 'oth', 'indiv'
    
    Returns:
        List of field names from official FEC header file
    """
    return get_schema().get_fields(file_type)


def parse_line(file_type: str, line: str, delimiter: str = '|') -> Dict[str, str]:
    """Convenience function to parse a line using default schema.
    
    Args:
        file_type: One of: 'cn', 'cm', 'ccl', 'pas2', 'oth', 'indiv'
        line: Pipe-delimited line from FEC bulk data file
        delimiter: Field delimiter (default: '|')
    
    Returns:
        Dict mapping field names to values
    """
    return get_schema().parse_line(file_type, line, delimiter)
