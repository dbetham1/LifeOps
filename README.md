# LifeOps

Personal data platform for ingesting, modelling and analysing health signals.

Stack:
- Python
- DuckDB
- Withings API
- GitHub Actions (planned)

## Git Commmands
| Command                                  | Description                                                              |
| ---------------------------------------- | ------------------------------------------------------------------------ |
| `git status`                             | Show current branch, staged/unstaged changes, untracked files.           |
| `git diff`                               | Show unstaged changes (what you edited but haven’t staged).              |
| `git diff --staged`                      | Show staged changes (what will be committed).                            |
| `git log --oneline --decorate -n 20`     | Show recent commit history (compact).                                    |
| `git add <file>`                         | Stage a specific file.                                                   |
| `git add .`                              | Stage all changes in the current directory (including new files).        |
| `git restore --staged <file>`            | Unstage a file (keep local edits).                                       |
| `git restore <file>`                     | Discard local unstaged changes to a file.                                |
| `git commit -m "message"`                | Create a commit with a message.                                          |
| `git commit --amend`                     | Edit the last commit (message and/or include additional staged changes). |
| `git branch`                             | List local branches (shows current).                                     |
| `git branch -a`                          | List local + remote branches.                                            |
| `git switch <branch>`                    | Switch to another branch (modern).                                       |
| `git checkout <branch>`                  | Switch branches (older command; still common).                           |
| `git switch -c <branch>`                 | Create and switch to a new branch.                                       |
| `git pull`                               | Fetch + merge from upstream tracking branch.                             |
| `git fetch`                              | Fetch remote updates without merging.                                    |
| `git push`                               | Push current branch to its upstream remote.                              |
| `git push -u origin <branch>`            | Push and set upstream tracking for the branch.                           |
| `git remote -v`                          | Show remote URLs (e.g., origin).                                         |
| `git remote show origin`                 | Show origin details (tracked branches, fetch/push URLs).                 |
| `git tag`                                | List tags.                                                               |
| `git tag -a v0.1.0 -m "msg"`             | Create an annotated tag.                                                 |
| `git push --tags`                        | Push tags to remote.                                                     |
| `git stash`                              | Temporarily save uncommitted changes.                                    |
| `git stash pop`                          | Re-apply the latest stash and remove it from stash list.                 |
| `git reset --hard HEAD`                  | Discard all local changes (dangerous).                                   |
| `git reset --soft HEAD~1`                | Undo last commit but keep changes staged.                                |
| `git rm --cached <file>`                 | Stop tracking a file (keep it locally), useful for secrets.              |
| `git rm -r --cached <dir>`               | Stop tracking a directory (keep it locally).                             |
| `git blame <file>`                       | Show who last changed each line in a file.                               |
| `git show <commit>`                      | Show a specific commit’s diff and metadata.                              |
| `git reflog`                             | Show local reference history (recover lost commits).                     |
| `git config --global user.name "Name"`   | Set your global git username.                                            |
| `git config --global user.email "email"` | Set your global git email.                                               |
| `git config --global core.autocrlf true` | Windows-friendly line ending handling (typical).                         |
| `git --no-pager diff`                    | Run diff without opening the pager (`less`).                             |
| `git config --global core.pager cat`     | Disable paging globally (not recommended unless you hate `less`).        |
