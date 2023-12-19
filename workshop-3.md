# Databricks Secret Management Labs

## Lab 1: Creating a Secret Scope in Databricks

### Objective
Learn to create a secret scope using the Databricks CLI.

### Steps
1. Understand the command to create a scope: `databricks secrets create-scope --scope <scope-name>`.
2. Create a scope with your chosen `<scope-name>`.
3. For non-Premium accounts, learn to override the default permission by granting the MANAGE permission to “users” (all users) using the command: `databricks secrets create-scope --scope <scope-name> --initial-manage-principal users`.

---

## Lab 2: Creating Secrets within a Scope

### Objective
Learn how to securely store sensitive information like tokens or passwords as secrets within a specified scope.

### Steps
1. Understand the importance of securely storing sensitive information.
2. Use the command `databricks secrets put --scope <scope-name> --key <key-name>` to create a secret.
3. Choose a `<scope-name>` and `<key-name>` for your secret, and store a piece of sensitive information.

---

## Lab 3: Deleting Secrets from a Scope

### Objective
Learn to delete a secret from a scope using the Databricks CLI.

### Steps
1. Understand scenarios where it might be necessary to delete secrets.
2. Use the command `databricks secrets delete --scope <scope-name> --key <key-name>` to delete a specific secret.
3. Select a secret previously created in Lab 2 and practice deleting it.

---

These labs provide hands-on experience in managing secrets within Databricks, an essential skill for maintaining data security and effective management.
