# Contributing to simplyblock-csi

🎉 Thanks for your interest in contributing!  

We welcome community contributions to improve **simplyblock-csi** and make it even better for Kubernetes users worldwide.  

This document outlines guidelines and steps to help you get started.

## 📌 Ways to Contribute

There are many ways to help:

- **Report bugs** — open an [issue](https://github.com/simplyblock/simplyblock-csi/issues) if you encounter problems.
- **Suggest features** — request improvements or new capabilities.
- **Improve documentation** — clarify instructions, fix typos, or add examples.
- **Submit code** — fix bugs, add features, or refactor code.
- **Review pull requests** — give constructive feedback on other contributions.

## 🐞 Reporting Issues

When reporting a bug, please include:

1. **Description** of the problem  
2. **Steps to reproduce** (if applicable)  
3. **Expected behavior** vs. what actually happened  
4. **Environment details**: Kubernetes distribution and version, Simplyblock version, OS, etc.  
5. Relevant **logs or error messages**

This helps us resolve issues faster.

## 🚀 Submitting Pull Requests

1. **Fork the repository**  
   Click “Fork” on the top-right of this repo and clone your fork locally.

   ```bash
   git clone https://github.com/<your-username>/simplyblock-csi.git
   cd simplyblock-csi
   ```

2. **Create a new branch**

   ```bash
   git checkout -b feature/my-feature
   ```

3. **Make your changes**
   Ensure your code follows project guidelines (see below).

4. **Commit your changes**

   ```bash
   git commit -m "Add: short description of the change"
   ```

   Use clear, concise commit messages (conventional commits are preferred but not required).

5. **Push to your fork**

   ```bash
   git push origin feature/my-feature
   ```

6. **Open a Pull Request**
   Go to the main repo and click **New Pull Request**.
   Provide context for your changes and link any related issues.

## 🧑‍💻 Coding Guidelines

To keep the project consistent:

* Follow standard **Python/Bash/Go** conventions (depending on the file).
* Keep code **readable and well-documented**.
* Write **small, focused commits**.
* Add or update **tests** where applicable.
* Run linting/formatting tools if defined (we may add CI checks in the future).

## ✅ Pull Request Checklist

Before submitting, please ensure:

* [ ] Code builds and runs locally
* [ ] Changes are tested (unit or integration tests where possible)
* [ ] Documentation updated if required
* [ ] Commit message(s) are descriptive
* [ ] PR references related issues (e.g. "Fixes #123")

## 🙌 Community Guidelines

* Be respectful and constructive.
* Provide context when suggesting changes.
* Help others by answering questions and reviewing PRs.

## 📬 Questions?

If you’re unsure about anything, feel free to:

* Create an [Issue](https://github.com/simplyblock/simplyblock-csi/issues)
* Reach out via the simplyblock community

💡 Together, we can make **simplyblock-csi** the best NVMe-first storage plugin for Kubernetes!
