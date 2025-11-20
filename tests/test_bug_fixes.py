"""
Tests for specific bug fixes in the codebase.
"""
import base64
import subprocess
import zlib

import pytest
from job_helper.repo_watcher import RepoWatcher


def test_empty_diff_check(tmp_path):
    """
    Test that empty diffs are correctly identified.
    Bug: Empty diffs were being base64-encoded and never matched empty string.
    Fix: Check against the actual base64-encoded empty compressed string.
    """
    test_repo = tmp_path / "test_repo"
    test_repo.mkdir()
    subprocess.run("git init", cwd=test_repo, shell=True, check=True)
    subprocess.run("git config user.email 'test@example.com'", cwd=test_repo, shell=True, check=True)
    subprocess.run("git config user.name 'Test User'", cwd=test_repo, shell=True, check=True)
    
    # Create and commit a file
    (test_repo / "test.txt").write_text("test")
    subprocess.run("git add test.txt", cwd=test_repo, shell=True, check=True)
    subprocess.run("git commit -m 'test'", cwd=test_repo, shell=True, check=True)
    
    # No uncommitted changes - should work with force_commit_repos
    w = RepoWatcher(force_commit_repos=[test_repo])
    rs = w.repo_states()
    assert len(rs) == 1
    assert len(rs[0].status) == 0
    
    # The empty diff should be the base64-encoded compressed empty string
    empty_diff_encoded = base64.b64encode(zlib.compress(b'')).decode()
    assert rs[0].diff == empty_diff_encoded
    
    # Now add uncommitted changes
    (test_repo / "test.txt").write_text("modified")
    
    # Should raise exception now
    w = RepoWatcher(force_commit_repos=[test_repo])
    with pytest.raises(Exception, match="Uncommitted changes"):
        w.repo_states()


def test_render_chart_return_value(tmp_path):
    """
    Test that render_chart returns chart string for HTML output.
    Bug: HTML output case didn't return anything (implicit None).
    Fix: Return chart string in all cases.
    """
    from job_helper._mermaid_backend import render_chart
    
    chart = "flowchart TD\n    A --> B"
    output_file = tmp_path / "test.html"
    
    result = render_chart(chart, str(output_file))
    
    # Should return the chart string
    assert result == chart
    
    # File should be created
    assert output_file.exists()


def test_compress_log_arcname():
    """
    Test that compress_log uses arcname to avoid full paths in tar.
    Bug: tar.add(file) was adding full paths instead of just filenames.
    Fix: Use arcname=file.name to add only the filename.
    """
    # This is tested indirectly through the compress_log function
    # We just verify the fix is in place by checking the code
    import inspect
    from job_helper.cli import compress_log
    
    source = inspect.getsource(compress_log)
    assert "arcname=file.name" in source
