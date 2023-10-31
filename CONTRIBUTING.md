# Contributing to Data I/O

Thank you for your interest in contributing to Data I/O! We appreciate your help in improving the project.

## Bug Reports and Feature Requests

If you encounter a bug or have a feature request, please open an issue on the [issue tracker][issues]. When opening an issue, please provide a clear description of the problem or the requested feature, along with any relevant information or examples.

## Pull Requests

We welcome pull requests for bug fixes, new features, and improvements to the project. To submit a pull request, please follow these guidelines:

1. Fork the repository and create a new branch for your contribution.
2. Make your changes, ensuring that you follow the project's coding style and guidelines.
3. Write tests to cover your changes whenever possible.
4. Commit your changes and push them to your forked repository.
5. Open a pull request to the main repository, describing the changes you've made and any additional information that may be relevant.

Please note that all contributions are subject to review. We appreciate your patience during the review process, and we may provide feedback or request changes before merging the pull request.

Once your pull request is approved, the integration of the new version is automatically handled using GitHub Actions.

## Development Setup

If you would like to set up a development environment for Data I/O, follow these steps:

1. Clone the repository
2. Build the project
3. Run tests to ensure everything is working

## Coding rules

To ensure consistency and code quality, please follow the following rules while preparing your pull request:

* Features and bug fixes **must be unit tested**
* Public API methods **must be documented**
* We use a scalafmt formatting file, so make sure to enable auto-formatting in your IDE to avoid
  inconsistencies in the code formatting

Make sure to follow the official [Scala style guideline](https://docs.scala-lang.org/style/).

<a name="auto-formatting"></a>

### Auto formatting

With IntelliJ:

1) In the **Settings/Preferences** dialog (Ctrl+Alt+S), go to **Editor | Code Style | Scala**
2) On the **Scalafmt** tab, in the **Formatter** field, select **scalafmt** to enable the formatter
3) Tick the **Reformat on file save** checkbox and click **Apply**

## Code of Conduct

Data I/O has adopted a [Code of Conduct][codeofconduct] to ensure a welcoming and inclusive community. Please review and adhere to the guidelines outlined in the Code of Conduct when participating in this project.

## License

Data I/O is released under the [Apache License 2.0][license]. By contributing to this project, you agree to license your contributions under the terms of the project's license.

[gettingstarted]: https://amadeusitgroup.github.io/dataio/getting-started.html
[documentation]: https://amadeusitgroup.github.io/dataio
[contributing]: CONTRIBUTING.md
[codeofconduct]: CODE_OF_CONDUCT.md
[license]: LICENSE
[repository]: https://github.com/AmadeusITGroup/conf4dataio
[issues]: https://github.com/AmadeusITGroup/conf4dataio/issues
