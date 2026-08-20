# FILE: src/contract_loader.py
# LIBRARY: ContractLoader — parses and validates a YAML data contract
# CONFIG: contract_path passed as parameter — no hardcoded paths
# __main__: none
# DIPANGGIL OLEH: run_pipeline.py

import yaml
from pathlib import Path
from dataclasses import dataclass


### Configuration
@dataclass
class ColumnSpec:
    name: str 
    type: str 
    nullable: bool 

@dataclass 
class SLASpec:
    freshness_hours: int
    delivery_time: str 
    timezone: str

@dataclass
class QualitySpec:
    completeness_threshold: float 
    accuracy_threshold: float 
    uniqueness_columns: list[str]

@dataclass 
class DataContract:
    name: str 
    version: str 
    owner: str 
    schema: list[ColumnSpec]
    sla: SLASpec
    quality: QualitySpec



### Create Data Contract
class ContractLoader:
    """Loads and validates a YAML data contract file."""

    ## data contract keys
    REQUIRED_TOP_KEYS = {"name", "version", "owner", "schema", "sla", "quality"}
    ## valid data types
    VALID_TYPES = {"string", "integer", "float", "timestamp", "boolean"}

    ## yaml file path
    def __init__(self, contract_path: str):
        self.contract_path = Path(contract_path)

    ## Load Data Contract (read, validate, parse)
    def load(self) -> DataContract:
        """Parse YAML and return a validated DataContract object."""
        raw = self._read_yaml()
        self._validate_structure(raw)
        return self._parse(raw)

    # 1. Read yaml file
    def _read_yaml(self) -> dict:
        if not self.contract_path.exists():
            raise FileNotFoundError(f"Contract file not found: {self.contract_path}")

        with open(self.contract_path, "r") as f:
            data = yaml.safe_load(f)

        return data.get("contract", {})

    # 2. Validate data structure 
    def _validate_structure(self, raw: dict) -> None:
        # missing contract keys
        missing = self.REQUIRED_TOP_KEYS - raw.keys()
        if missing: 
            raise ValueError(f"Contract missing required keys: {missing}")

        for col in raw["schema"]:
            if col["type"] not in self.VALID_TYPES:
                raise ValueError(
                    f"Column '{col['name']}' has invalid type '{col['type']}'. "
                    f"Valid types: {self.VALID_TYPES}"
                )

    # 3. Parse data contract
    def _parse(self, raw: dict) -> DataContract:
        schema = [
            ColumnSpec(
                name=col["name"],
                type=col["type"],
                nullable=col["nullable"],
            )
            for col in raw["schema"]
        ]

        sla = SLASpec(
            freshness_hours=raw["sla"]["freshness_hours"],
            delivery_time=raw["sla"]["delivery_time"],
            timezone=raw["sla"]["timezone"],
        )

        quality = QualitySpec(
            completeness_threshold=raw["quality"]["completeness_threshold"],
            accuracy_threshold=raw["quality"]["accuracy_threshold"],
            uniqueness_columns=raw["quality"]["uniqueness_columns"],
        )

        return DataContract(
            name=raw["name"],
            version=raw["version"],
            owner=raw["owner"],
            schema=schema,
            sla=sla,
            quality=quality,
        )