import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import call, patch

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


class DependencyTests(unittest.TestCase):
    root = Path("/repo")

    @staticmethod
    def issue(state="CLOSED", references=None):
        if references is None:
            references = [
                {
                    "id": "PR_linked",
                    "number": 8,
                    "repository": {
                        "id": "R_repo",
                        "name": "hyperliquid-sim",
                        "owner": {"id": "U_owner", "login": "kfastov"},
                    },
                    "url": "https://github.com/kfastov/hyperliquid-sim/pull/8",
                }
            ]
        return {"closedByPullRequestsReferences": references, "state": state}

    @patch.object(gpp, "gh_json")
    def test_closed_issue_with_merged_linked_pr_is_accepted(self, mock_gh_json):
        mock_gh_json.side_effect = [
            self.issue(),
            {
                "mergeCommit": {"oid": "723c5f7"},
                "mergedAt": "2026-07-15T11:57:25Z",
                "state": "MERGED",
            },
        ]

        gpp.validate_dependencies(self.root, [1])

        self.assertEqual(
            mock_gh_json.call_args_list,
            [
                call(
                    [
                        "issue",
                        "view",
                        "1",
                        "--json",
                        "state,closedByPullRequestsReferences",
                    ],
                    self.root,
                ),
                call(
                    ["pr", "view", "8", "--json", "state,mergedAt,mergeCommit"],
                    self.root,
                ),
            ],
        )

    @patch.object(gpp, "gh_json")
    def test_closed_issue_with_only_closed_unmerged_pr_is_rejected(self, mock_gh_json):
        mock_gh_json.side_effect = [
            self.issue(),
            {"mergeCommit": None, "mergedAt": None, "state": "CLOSED"},
        ]

        with self.assertRaisesRegex(gpp.ProtocolError, "not accepted by merged PR"):
            gpp.validate_dependencies(self.root, [1])

        self.assertEqual(mock_gh_json.call_count, 2)

    @patch.object(gpp, "gh_json")
    def test_open_issue_is_rejected_without_pr_lookup(self, mock_gh_json):
        mock_gh_json.return_value = self.issue(
            state="OPEN",
            references=[{"number": "malformed", "url": "https://example.invalid"}],
        )

        with self.assertRaisesRegex(gpp.ProtocolError, "Issue is not closed"):
            gpp.validate_dependencies(self.root, [1])

        mock_gh_json.assert_called_once()

    @patch.object(gpp, "gh_json")
    def test_lookup_failure_is_an_explicit_protocol_error(self, mock_gh_json):
        mock_gh_json.side_effect = gpp.ProtocolError("command failed (1): gh issue view")

        with self.assertRaisesRegex(gpp.ProtocolError, "dependency #1 lookup failed"):
            gpp.validate_dependencies(self.root, [1])

    @patch.object(gpp, "gh_json")
    def test_pr_schema_failure_is_an_explicit_protocol_error(self, mock_gh_json):
        mock_gh_json.side_effect = [
            self.issue(),
            {"mergeCommit": {"oid": "723c5f7"}, "state": "MERGED"},
        ]

        with self.assertRaisesRegex(gpp.ProtocolError, "linked PR #8 has invalid schema"):
            gpp.validate_dependencies(self.root, [1])


if __name__ == "__main__":
    unittest.main()
