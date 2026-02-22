# Contributing to BLite Server

Thank you for your interest in contributing to **BLite Server**! We appreciate your help in making this database engine faster and more efficient.

## Our License Model and the CLA

BLite Server is distributed under the **GNU AGPLv3** for the community. However, to ensure the long-term sustainability of the project, we also offer a **commercial license** (dual-licensing model).

### 1. Contributor License Agreement (CLA)

To accept your code contributions, we require all contributors to sign our **Individual Contributor License Agreement (CLA)**.

-   **Why?** This allows the project owner (Luca Fabbri) to sublicense the code under commercial terms while keeping the community version open source.
    
-   **How?** When you open a Pull Request, our **CLA Assistant** bot will automatically check if you have signed the agreement. If not, follow the link provided by the bot to sign it via GitHub.
    

## Technical Guidelines

### Clean Architecture & DDD

We follow strict **Clean Architecture** and **Domain-Driven Design (DDD)** principles. When submitting code:

-   Ensure a clear separation between Domain, Application, and Infrastructure layers.
    
-   Keep the Domain layer free of external dependencies.
    
-   Respect existing patterns for BSON serialization and the 16KB page management system.
    

### Code Documentation

-   All public methods and classes must be documented in **English**.
    
-   Comments should explain the "why" rather than the "what".
    

## How to Contribute

### Reporting Bugs

1.  Check the Issue Tracker to see if the bug has already been reported.
    
2.  If not, open a new issue with a clear title and a detailed description, including steps to reproduce the bug.
    

### Suggesting Enhancements

1.  Open an issue to discuss the proposed feature before starting any implementation.
    
2.  Explain the use case and the technical impact on performance (especially regarding I/O and page density).
    

### Pull Request Process

1.  **Fork** the repository and create your branch from `main`.
    
2.  Follow the architectural patterns mentioned above.
    
3.  Ensure your code passes all existing tests.
    
4.  Open a **Pull Request**.
    
5.  **Sign the CLA** via the link provided by the CLA Assistant bot.
    
6.  Wait for a code review from the maintainers.
    

## Contact

If you have questions regarding the contribution process or the commercial license, please contact **Luca Fabbri**.