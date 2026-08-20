import pytest
from src.contract_loader import ContractLoader
from src.contract_enforcer import ContractEnforcer


## 1. Test Contract Load
def test_contract_loads(tmp_path):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  version: 1.0.0
  owner: test-team

  schema:
  - name: order_id
    type: string
    nullable: false
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns:
      - order_id
""")

    # load dummy data contract
    contract = ContractLoader(contract_path=str(contract_file)).load()

    # assert the valid result
    assert contract.name == "test_pipeline"
    assert len(contract.schema) == 1
    assert contract.sla.freshness_hours == 24



## 2. Test Missing Field (should raise Error)
def test_missing_field(tmp_path):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  owner: test-team

  schema: []
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns: []
""")

    with pytest.raises(ValueError):
        ContractLoader(str(contract_file)).load()



## 3. Test Invalid Column Types (should raise Error)
def test_invalid_column_type(tmp_path):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  version: 1.0.0
  owner: test-team

  schema:
  - name: order_id
    type: string
    nullable: false
  - name: customer_id
    type: int
    nullable: true
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns:
      - order_id
""")

    with pytest.raises(ValueError):
        ContractLoader(str(contract_file)).load()



## 4. Test Required Columns
def test_required_columns(tmp_path):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  version: 1.0.0
  owner: test-team

  schema:
  - name: order_id
    type: string
    nullable: false
  - name: customer_id
    type: string
    nullable: false
  - name: quantity
    type: integer
    nullable: true
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns:
      - order_id
""")

    # load dummy data contract
    contract = ContractLoader(contract_path=str(contract_file)).load()

    # enforce data contract to pipeline
    contract_enforce = ContractEnforcer(contract)

    # assert the valid result
    required_columns1 = [col.name for col in contract.schema]
    required_columns2 = contract_enforce.get_required_columns()
    assert required_columns1 == required_columns2



## 5. Test Non-nullable Columns
def test_non_nullable_columns(tmp_path):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  version: 1.0.0
  owner: test-team

  schema:
  - name: order_id
    type: string
    nullable: false
  - name: customer_id
    type: string
    nullable: false
  - name: quantity
    type: integer
    nullable: true
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns:
      - order_id
""")

    # load dummy data contract
    contract = ContractLoader(contract_path=str(contract_file)).load()

    # enforce data contract to pipeline
    contract_enforce = ContractEnforcer(contract)

    # assert valid data
    non_nullable_cols1 = [col.name for col in contract.schema if not col.nullable]
    non_nullable_cols2 = contract_enforce.get_non_nullable_columns()
    assert non_nullable_cols1 == non_nullable_cols2



## 6. Test Breaking Change Warning
def test_breaking_change_warning(tmp_path, capsys):
    # create dummy yaml file in tmp folder
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
contract:
  name: test_pipeline
  version: 2.0.0
  previous_version: 1.0.0
  breaking_change: true
  change_description: "Removed customer_id column"
  owner: test-team

  schema:
  - name: order_id
    type: string
    nullable: false
  - name: quantity
    type: integer
    nullable: true
    
  sla:
    freshness_hours: 24
    delivery_time: "07:00"
    timezone: "Asia/Jakarta"

  quality:
    completeness_threshold: 0.95
    accuracy_threshold: 0.90
    uniqueness_columns:
      - order_id
""")

    # load dummy data contract
    contract = ContractLoader(contract_path=str(contract_file)).load()

    # enforce data contract to pipeline
    contract_enforce = ContractEnforcer(contract)

    # check breaking change warning
    contract_enforce.check_breaking_change()

    # capture what its printed
    captured = capsys.readouterr()
    assert "BREAKING CHANGE DETECTED" in captured.out
    assert "1.0.0 -> 2.0.0" in captured.out

