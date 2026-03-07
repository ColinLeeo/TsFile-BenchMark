import os
import re
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"}


class GitPublishError(RuntimeError):
    pass


def _run_git(repo_root: Path, args: List[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise GitPublishError(result.stderr.strip() or "git command failed")
    return result.stdout.strip()


def _parse_repo_slug(remote_url: str) -> str:
    normalized = remote_url.strip()
    ssh_match = re.search(r"github\.com[:/]([^/]+/[^/]+?)(?:\.git)?$", normalized)
    if ssh_match:
        return ssh_match.group(1)
    https_match = re.search(r"https?://github\.com/([^/]+/[^/]+?)(?:\.git)?$", normalized)
    if https_match:
        return https_match.group(1)
    raise GitPublishError(f"Cannot parse GitHub repo slug from remote URL: {remote_url}")


def publish_result_assets(
    *,
    repo_root: str,
    source_dir: str = "result",
    archive_base_dir: str = "benchmark_result",
    remote: str = "origin",
    repo_slug: str = "",
    commit_message: str = "chore(benchmark): publish benchmark assets",
    auto_commit: bool = True,
) -> Dict[str, object]:
    """
    Archive benchmark results from source_dir to archive_base_dir with timestamp.
    Optionally commits to current branch (no branch switching).
    
    Args:
        repo_root: Repository root path
        source_dir: Temporary directory with benchmark results (default: "result")
        archive_base_dir: Archive directory for historical results (default: "benchmark_result")
        remote: Git remote name (default: "origin")
        repo_slug: GitHub repo slug (e.g., "user/repo")
        commit_message: Git commit message
        auto_commit: Whether to auto-commit and push (default: True). If False, only stages files.
    
    Returns:
        Dict with archive_dir and image_urls. commit_sha only available when auto_commit=True.
    """
    root = Path(repo_root).resolve()
    source_path = (root / source_dir).resolve()
    archive_base = (root / archive_base_dir).resolve()
    
    if not source_path.exists():
        raise FileNotFoundError(f"Source directory not found: {source_path}")

    # Collect files from source directory (only direct files, not subdirectories)
    json_files: List[Path] = []
    image_files: List[Path] = []
    
    for item in source_path.iterdir():
        if not item.is_file():
            continue
        suffix = item.suffix.lower()
        if suffix == ".json":
            json_files.append(item)
        elif suffix in IMAGE_EXTENSIONS:
            image_files.append(item)
    
    if not json_files and not image_files:
        raise FileNotFoundError(f"No JSON or image files found in {source_path}")

    # Verify we're in a git repository
    _run_git(root, ["rev-parse", "--is-inside-work-tree"])
    
    # Generate timestamp for archive directory
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_dir = archive_base / timestamp
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy files to archive directory
    for file_path in json_files + image_files:
        shutil.copy(file_path, archive_dir / file_path.name)
    
    print(f"📦 Archived {len(json_files)} JSON and {len(image_files)} image files to {archive_base_dir}/{timestamp}/")
    
    # Save original remote URL to restore later
    original_remote_url = None
    try:
        original_remote_url = _run_git(root, ["remote", "get-url", remote])
    except GitPublishError:
        pass

    try:
        # Git add archived directory
        relative_archive_path = f"{archive_base_dir}/{timestamp}"
        _run_git(root, ["add", relative_archive_path])
        print(f"✅ Staged archived files for commit")

        # If auto_commit is False, return early without committing
        if not auto_commit:
            return {
                "archive_dir": timestamp,
                "image_urls": {},
            }

        # Check if there are changes to commit
        has_staged_changes = True
        try:
            _run_git(root, ["diff", "--cached", "--quiet"])
            has_staged_changes = False
        except GitPublishError:
            has_staged_changes = True

        if not has_staged_changes:
            print(f"⚠️ No changes to commit")
            return {
                "commit_sha": _run_git(root, ["rev-parse", "HEAD"]),
                "archive_dir": timestamp,
                "image_urls": {},
            }

        # Commit changes
        _run_git(root, ["commit", "-m", f"{commit_message} ({timestamp})"])
        print(f"✅ Committed archived results")

        # Parse repo slug if not provided
        github_token = os.getenv("GITHUB_TOKEN")
        if not repo_slug:
            try:
                remote_url = _run_git(root, ["remote", "get-url", remote])
                repo_slug = _parse_repo_slug(remote_url)
            except GitPublishError:
                raise GitPublishError("Cannot determine repository slug. Please provide repo_slug parameter.")
        
        # Set authenticated remote URL with token before pushing
        if github_token and repo_slug:
            auth_url = f"https://{github_token}@github.com/{repo_slug}.git"
            _run_git(root, ["remote", "set-url", remote, auth_url])
            print(f"✅ Set authenticated remote URL for {remote}")
        else:
            raise GitPublishError("GITHUB_TOKEN or repo_slug not available for authentication")

        # Push to remote
        current_branch = _run_git(root, ["rev-parse", "--abbrev-ref", "HEAD"])
        _run_git(root, ["push", remote, current_branch])
        commit_sha = _run_git(root, ["rev-parse", "HEAD"])
        print(f"✅ Pushed to {remote}/{current_branch} (commit: {commit_sha[:8]})")

        # Build image URLs pointing to archived files
        def build_raw_url(filename: str) -> str:
            return f"https://raw.githubusercontent.com/{repo_slug}/{commit_sha}/{archive_base_dir}/{timestamp}/{filename}"

        image_urls = {
            f"{timestamp}/{img.name}": build_raw_url(img.name)
            for img in image_files
        }

        return {
            "commit_sha": commit_sha,
            "archive_dir": timestamp,
            "image_urls": image_urls,
        }
    finally:
        # Restore original remote URL if it was changed
        if original_remote_url:
            try:
                _run_git(root, ["remote", "set-url", remote, original_remote_url])
            except GitPublishError:
                pass
        
        print(f"✅ Benchmark results archived and published")