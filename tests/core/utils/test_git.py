from unittest.mock import MagicMock, patch

import pytest

from dbt_ddc_generator.core.utils.git import GitOperations


@pytest.fixture
def mock_git_ops(monkeypatch, tmp_path):
    """Create GitOperations with mocked directory."""
    monkeypatch.setenv("carrot_directory", str(tmp_path))
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
    return GitOperations()


def test_write_to_files(mock_git_ops):
    """Test writing check files."""
    checks = [
        {"type": "duplicates", "content": "test content"},
        {"type": "completeness", "content": "test content"},
    ]

    result = mock_git_ops.write_to_files("test_model", checks)
    assert result


@patch("subprocess.run")
def test_create_branch_from_master(mock_run, mock_git_ops):
    """Test creating new branch."""
    mock_run.return_value = MagicMock(stdout="")

    mock_git_ops.create_branch_from_master("test-branch")

    # Verify git commands were called
    assert mock_run.call_count >= 4  # Should call multiple git commands
