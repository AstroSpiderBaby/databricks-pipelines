
# 🧠 Git Workflow Cheat Sheet (Databricks + VS Code)

This guide helps you avoid merge conflicts and keep your Databricks and local VS Code environments in sync.

---

## 🔁 General Workflow: One Environment at a Time

1. ✅ **Make changes in only ONE environment at a time**
2. 📤 **Commit and push** when you're done:
   ```bash
   git add .
   git commit -m "Describe your changes"
   git push origin main
   ```

3. ⬇️ **Pull updates** before switching to another environment:
   ```bash
   git pull origin main
   ```

---

## ✅ Safe Steps When Switching Environments

### 🔎 Check local status:
```bash
git status
```

### 💾 Stash if needed:
```bash
git stash
# Then after pulling:
git stash pop
```

---

## 🧪 Common Commands

| Action                        | Command                                       |
|------------------------------|-----------------------------------------------|
| Clone repo                   | `git clone <repo-url>`                        |
| See status                   | `git status`                                  |
| Stage all changes            | `git add .`                                   |
| Commit changes               | `git commit -m "Your message"`                |
| Push to remote               | `git push origin main`                        |
| Pull latest from remote      | `git pull origin main`                        |
| See commit history           | `git log --oneline`                           |
| Stash changes (temp save)    | `git stash`                                   |
| Apply stashed changes        | `git stash pop`                               |
| Resolve merge conflict       | Open file ➜ fix manually ➜ `git add <file>`   |

---

## 🧯 Conflict Recovery (If it happens)

If you get a conflict while pulling:
```bash
# Open conflicted files, resolve them manually
git add <resolved-files>
git commit
git push
```

---

## 📌 Best Practices

- Always pull before working
- Avoid working on both environments at once
- Commit often with meaningful messages
- Use branches for experimentation

---

_Keep this file in `/docs/git_cheat_sheet.md` for quick reference._
