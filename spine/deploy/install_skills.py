# Databricks notebook source
# MAGIC %md
# MAGIC # Install migration skills into Genie Code
# MAGIC Uploads each `skills/<name>/` folder (SKILL.md + references/) to the workspace so
# MAGIC Genie Code can invoke them. Target: workspace-level `/Workspace/.assistant/skills/`
# MAGIC (shared) or user-level `/Users/<you>/.assistant/skills/` (personal demo).

# COMMAND ----------

dbutils.widgets.dropdown("scope", "user", ["user", "workspace"])
dbutils.widgets.text("skills_source_dir", "../../skills")

scope = dbutils.widgets.get("scope")
skills_source_dir = dbutils.widgets.get("skills_source_dir")

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

if scope == "workspace":
    dest_root = "/Workspace/.assistant/skills"
else:
    me = w.current_user.me().user_name
    dest_root = f"/Users/{me}/.assistant/skills"

print(f"Installing skills to: {dest_root}")

# COMMAND ----------

def upload_tree(local_dir: str, ws_dir: str):
    """Recursively upload a local folder to a workspace path."""
    for entry in sorted(os.listdir(local_dir)):
        lpath = os.path.join(local_dir, entry)
        wpath = f"{ws_dir}/{entry}"
        if os.path.isdir(lpath):
            w.workspace.mkdirs(wpath)
            upload_tree(lpath, wpath)
        else:
            with open(lpath, "rb") as fh:
                content = fh.read()
            # Upload as a raw file (AUTO import so .md stays .md).
            w.workspace.upload(
                path=wpath,
                content=content,
                format="RAW",
                overwrite=True,
            )
            print(f"  uploaded {wpath}")

# COMMAND ----------

skill_names = [
    d for d in sorted(os.listdir(skills_source_dir))
    if os.path.isdir(os.path.join(skills_source_dir, d))
    and os.path.exists(os.path.join(skills_source_dir, d, "SKILL.md"))
]
print("Skills to install:", skill_names)

w.workspace.mkdirs(dest_root)
for name in skill_names:
    print(f"Installing {name} ...")
    dest = f"{dest_root}/{name}"
    w.workspace.mkdirs(dest)
    upload_tree(os.path.join(skills_source_dir, name), dest)

print("Done. Reload Genie Code to pick up the skills.")
