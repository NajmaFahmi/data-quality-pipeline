# FILE: src/contract_enforcer.py
# LIBRARY: ContractEnforcer — enforce rules for Gate 1, Gate 3, Gate 4, and Version Check
# CONFIG: contract passed as DataContract — no hardcoded contract
# __main__: none
# DIPANGGIL OLEH: run_pipeline.py


from src.contract_loader import DataContract  


class ContractEnforcer:
    def __init__(self, contract: DataContract):
        self.contract = contract 
    
    # Required Columns
    def get_required_columns(self) -> list[str]:
        """Returns the required column name"""
        return [col.name for col in self.contract.schema]

    # Non-nullable Columns
    def get_non_nullable_columns(self) -> list[str]:
        """Returns the non-nullable column name"""
        return [col.name for col in self.contract.schema if not col.nullable]

    # Quality Thresholds
    def get_quality_thresholds(self) -> dict:
        """Return threshold for completeness and accuracy, also uniqueness columns"""
        completeness = self.contract.quality.completeness_threshold
        accuracy = self.contract.quality.accuracy_threshold
        uniqueness = [col for col in self.contract.quality.uniqueness_columns]
        return {"completeness_threshold": completeness,
                "accuracy_threshold": accuracy,
                "uniqueness_columns": uniqueness}

    # Check Breaking Change
    def check_breaking_change(self) -> None:
        if self.contract.breaking_change:
            print("=" * 55)
            print("WARNING: BREAKING CHANGE DETECTED")
            print(f"  Contract : {self.contract.name}")
            print(f"  Version  : {self.contract.previous_version} -> {self.contract.version}")
            print("  Review downstream systems before proceeding.")
            print("=" * 55)