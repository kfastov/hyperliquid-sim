import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

MODULE_PATH = Path(__file__).parents[1] / "tools" / "gpp.py"
spec = importlib.util.spec_from_file_location("gpp", MODULE_PATH)
assert spec and spec.loader
gpp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gpp)


class ContractTests(unittest.TestCase):
    def valid(self):
        return {
            "protocol": "gpp/0.1",
            "issue": 7,
            "objective": "observable outcome",
            "scope": {"write": ["crates/core/**"], "forbidden": [".github/**"]},
            "inputs": ["AGENTS.md"],
            "dependencies": [],
            "acceptance": {"commands": ["cargo test"], "assertions": ["tests pass"]},
            "deliverables": ["matching engine"],
            "risks": [],
        }

    def test_valid_contract(self):
        gpp.validate_contract_data(self.valid())

    def test_unknown_field_rejected(self):
        data = self.valid()
        data["status"] = "done"
        with self.assertRaisesRegex(gpp.ProtocolError, "unknown fields"):
            gpp.validate_contract_data(data)

    def test_filename_must_match_issue(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "8.json"
            path.write_text(json.dumps(self.valid()))
            with self.assertRaisesRegex(gpp.ProtocolError, "filename must be 7.json"):
                gpp.validate_contract(path)

    def test_empty_acceptance_rejected(self):
        data = self.valid()
        data["acceptance"]["commands"] = []
        with self.assertRaisesRegex(gpp.ProtocolError, "must not be empty"):
            gpp.validate_contract_data(data)


class MetadataTests(unittest.TestCase):
    def test_parses_single_block(self):
        body = '<!-- gpp {"task":7,"mode":"strict","contract":"protocol/tasks/7.json","context_digest":"sha256:' + "a" * 64 + '"} -->'
        self.assertEqual(gpp.parse_metadata(body)["task"], 7)

    def test_duplicate_blocks_rejected(self):
        block = '<!-- gpp {"task":7,"mode":"light","context_digest":"sha256:' + "a" * 64 + '"} -->'
        with self.assertRaisesRegex(gpp.ProtocolError, "exactly one"):
            gpp.parse_metadata(block + block)

    def test_wrong_contract_path_rejected(self):
        body = '<!-- gpp {"task":7,"mode":"strict","contract":"protocol/tasks/8.json","context_digest":"sha256:' + "a" * 64 + '"} -->'
        with self.assertRaisesRegex(gpp.ProtocolError, "contract path"):
            gpp.parse_metadata(body)


class ScopeTests(unittest.TestCase):
    def test_allowed_scope(self):
        contract = self_contract = ContractTests().valid()
        gpp.validate_paths(["crates/core/src/lib.rs"], self_contract)

    def test_out_of_scope_rejected(self):
        contract = ContractTests().valid()
        with self.assertRaisesRegex(gpp.ProtocolError, "outside task write scope"):
            gpp.validate_paths(["README.md"], contract)

    def test_forbidden_wins(self):
        contract = ContractTests().valid()
        contract["scope"]["write"].append(".github/**")
        with self.assertRaisesRegex(gpp.ProtocolError, "forbidden paths"):
            gpp.validate_paths([".github/workflows/ci.yml"], contract)


if __name__ == "__main__":
    unittest.main()
