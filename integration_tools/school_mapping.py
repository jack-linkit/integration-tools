"""
School mapping utilities for fixing integration errors.
"""

import json
from typing import Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import School, DistrictDataParm


def interactive_school_mapping_fix(
    db: Session,
    district_id: int,
    import_types: List[str],
    json_path: str,
    integration_name: str
) -> bool:
    """
    Interactive school mapping fix for integration errors.
    
    Args:
        db: Database session
        district_id: District ID to fix
        import_types: List of import types to check
        json_path: JSON path to school mappings in config
        integration_name: Name of the integration
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"\n{integration_name} School Mapping Fix for District {district_id}")
        print("=" * 60)
        
        # Get all DistrictDataParm records for this integration
        records = db.query(DistrictDataParm).filter(
            DistrictDataParm.DistrictID == district_id,
            DistrictDataParm.ImportType.in_(import_types)
        ).all()
        
        if not records:
            print(f"No DistrictDataParm records found for District {district_id}")
            return False
        
        # Get all schools for this district
        schools = db.query(School).filter(
            School.DistrictID == district_id,
            School.Status == 1
        ).order_by(School.Name).all()
        
        if not schools:
            print(f"No active schools found for District {district_id}")
            return False
        
        print(f"Found {len(schools)} active schools:")
        for i, school in enumerate(schools, 1):
            print(f"  {i:2d}. {school.Name} (Code: {school.Code})")
        
        # Process each record
        updated_count = 0
        for record in records:
            try:
                config_data = json.loads(record.JSONDataConfig or "{}")
                
                # Navigate to school mappings using json_path
                current = config_data
                path_parts = json_path.replace("$.", "").split(".")
                
                for part in path_parts:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                
                # Current should now be the school mappings object
                print(f"\nProcessing {record.ImportType}:")
                print(f"Current school mappings: {json.dumps(current, indent=2)}")
                
                # Ask user if they want to update this record
                update = input(f"Update school mappings for {record.ImportType}? (y/n): ").lower()
                
                if update == 'y':
                    # Interactive mapping update
                    print("\nSchool mapping options:")
                    print("1. Clear all mappings")
                    print("2. Add/update specific mapping")
                    print("3. Skip")
                    
                    choice = input("Select option (1-3): ").strip()
                    
                    if choice == '1':
                        # Clear all mappings
                        current.clear()
                        print("All school mappings cleared")
                        
                    elif choice == '2':
                        # Add/update specific mapping
                        external_id = input("Enter external school ID: ").strip()
                        if external_id:
                            print("\nSelect LinkIt school:")
                            for i, school in enumerate(schools, 1):
                                print(f"  {i:2d}. {school.Name} (ID: {school.SchoolID}, Code: {school.Code})")
                            
                            try:
                                school_choice = int(input("Enter school number: ")) - 1
                                if 0 <= school_choice < len(schools):
                                    selected_school = schools[school_choice]
                                    current[external_id] = selected_school.SchoolID
                                    print(f"Mapped '{external_id}' to '{selected_school.Name}' (ID: {selected_school.SchoolID})")
                                else:
                                    print("Invalid school selection")
                            except ValueError:
                                print("Invalid input")
                    
                    # Update the record
                    record.JSONDataConfig = json.dumps(config_data)
                    db.commit()
                    updated_count += 1
                    print(f"Updated {record.ImportType} configuration")
                
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON for {record.ImportType}: {e}")
                continue
            except Exception as e:
                print(f"Error processing {record.ImportType}: {e}")
                continue
        
        print(f"\nCompleted: Updated {updated_count}/{len(records)} records")
        return updated_count > 0
        
    except Exception as e:
        print(f"Error in school mapping fix: {e}")
        return False


def get_school_mapping_errors(db: Session, district_id: int) -> Dict[str, List[str]]:
    """
    Get school mapping errors for a district.
    
    Args:
        db: Database session
        district_id: District ID
        
    Returns:
        Dictionary of import type -> list of error messages
    """
    errors = {}
    
    try:
        # Query for recent errors (this would need to be customized based on your error logging)
        sql = text("""
            SELECT ImportType, ErrorMessage 
            FROM ImportErrors 
            WHERE DistrictID = :district_id 
            AND ErrorMessage LIKE '%school%mapping%'
            ORDER BY CreatedDate DESC
        """)
        
        result = db.execute(sql, {"district_id": district_id})
        
        for row in result:
            import_type = row.ImportType
            if import_type not in errors:
                errors[import_type] = []
            errors[import_type].append(row.ErrorMessage)
    
    except Exception as e:
        print(f"Could not retrieve school mapping errors: {e}")
    
    return errors