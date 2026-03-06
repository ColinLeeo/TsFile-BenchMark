import os
import re
import subprocess
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple


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


def _collect_assets(base_dir: Path) -> Tuple[List[Path], List[Path]]:
    json_files: List[Path] = []
    image_files: List[Path] = []
    for path in base_dir.rglob("*"):
        if not path.is_file():
            continue
        lower_suffix = path.suffix.lower()
        if lower_suffix == ".json":
            json_files.append(path)
        elif lower_suffix in IMAGE_EXTENSIONS:
            image_files.append(path)
    json_files.sort()
    image_files.sort()
    return json_files, image_files


def publish_result_assets(
    *,
    repo_root: str,
    branch: str,
    base_dir: str = "result",
    remote: str = "origin",
    repo_slug: str = "",
    commit_message: str = "chore(benchmark): publish benchmark assets",
) -> Dict[str, object]:
    """
    Commit benchmark JSON + image files from base_dir and push to a target branch.
    Returns image URLs based on commit SHA.
    """
    root = Path(repo_root).resolve()
    assets_dir = (root / base_dir).resolve()
    if not assets_dir.exists():
        raise FileNotFoundError(f"Assets directory not found: {assets_dir}")

    json_files, image_files = _collect_assets(assets_dir)
    if not json_files and not image_files:
        raise FileNotFoundError(f"No JSON or image files found under {assets_dir}")

    _run_git(root, ["rev-parse", "--is-inside-work-tree"])
    original_ref = _run_git(root, ["rev-parse", "--abbrev-ref", "HEAD"])
    
    # Save original remote URL to restore later
    original_remote_url = None
    try:
        original_remote_url = _run_git(root, ["remote", "get-url", remote])
    except GitPublishError:
        pass
    
    files_to_add = json_files + image_files

    # Create temporary directory to store files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Copy files to temporary directory
        for file_path in files_to_add:
            relative_path = file_path.relative_to(root)
            dest_path = temp_path / relative_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                # Use copy() instead of copy2() to avoid permission issues with root-owned files
                shutil.copy(file_path, dest_path)
            except PermissionError as e:
                raise GitPublishError(f"Permission denied copying {file_path}. Files may be owned by root. Run: sudo chown -R $USER:$USER {file_path.parent}")

        try:
            branch_exists = True
            try:
                _run_git(root, ["rev-parse", "--verify", branch])
            except GitPublishError:
                branch_exists = False

            if branch_exists:
                _run_git(root, ["checkout", branch])
            else:
                _run_git(root, ["checkout", "-b", branch])

            # Copy files from temp directory back to repo
            for file_path in files_to_add:
                relative_path = file_path.relative_to(root)
                source_path = temp_path / relative_path
                dest_path = root / relative_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_path, dest_path)

            relative_files = [str(path.relative_to(root)).replace(os.sep, "/") for path in files_to_add]
            _run_git(root, ["add", *relative_files])

            has_staged_changes = True
            try:
                _run_git(root, ["diff", "--cached", "--quiet"])
                has_staged_changes = False
            except GitPublishError:
                has_staged_changes = True

            if has_staged_changes:
                _run_git(root, ["commit", "-m", commit_message])

            # Set authenticated remote URL with token before pushing
            github_token = os.getenv("GITHUB_TOKEN")
            if not repo_slug:
                # Try to parse from existing remote URL
                try:
                    remote_url = _run_git(root, ["remote", "get-url", remote])
                    repo_slug = _parse_repo_slug(remote_url)
                except GitPublishError:
                    raise GitPublishError("Cannot determine repository slug. Please provide repo_slug parameter.")
            
            if github_token and repo_slug:
                auth_url = f"https://{github_token}@github.com/{repo_slug}.git"
                _run_git(root, ["remote", "set-url", remote, auth_url])
                print(f"✅ Set authenticated remote URL for {remote}")
            else:
                raise GitPublishError("GITHUB_TOKEN or repo_slug not available for authentication")

            _run_git(root, ["push", "-u", remote, branch])
            commit_sha = _run_git(root, ["rev-parse", "HEAD"])

            def build_raw_url(path: Path) -> str:
                relative_path = str(path.relative_to(root)).replace(os.sep, "/")
                return f"https://raw.githubusercontent.com/{repo_slug}/{commit_sha}/{relative_path}"

            image_urls = {str(path.relative_to(root)).replace(os.sep, "/"): build_raw_url(path) for path in image_files}

            return {
                "branch": branch,
                "commit_sha": commit_sha,
                "image_urls": image_urls,
            }
        finally:
            # Restore original remote URL if it was changed
            if original_remote_url:
                try:
                    _run_git(root, ["remote", "set-url", remote, original_remote_url])
                except GitPublishError:
                    pass
            
            # Restore original branch
            current_ref = _run_git(root, ["rev-parse", "--abbrev-ref", "HEAD"])
            if current_ref != original_ref:
                _run_git(root, ["checkout", original_ref])