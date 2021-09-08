# NHSX AU-Data Engineering - Azure Databricks Analytics

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

[![Contributors][contributors-shield]][contributors-url]
[![Code Lines][code-lines]][code-lines-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]


<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/nhsx/au-azure-databricks">
    <img src="img/databricks-logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Azure Databricks Analytics</h3>

  <p align="center">
    NHSX Analytics Unit - Data Engineering Team
    <br />
    <a href="https://nhsx.github.io/au-data-engineering/"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/nhsx/au-azure-databricks/issues">Report Bug</a>
    ·
    <a href="https://github.com/nhsx/au-azure-databricks/issues">Request Feature</a>
  </p>
</p>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <!-- <li><a href="#prerequisites">Prerequisites</a></li> -->
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <!-- <li><a href="#acknowledgements">Acknowledgements</a></li> -->
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## About The Project

NHSX Analytics Unit - Data Engineering Team. Databricks analytics repository. Contains ETL pipeline notebooks for NHSX analytics projects.

_**Note:** No data, public or private are shared in this repository._

### Folder Stucture

| Name                   | Link                                                                                    | Description                                                     |
| ---------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Orchestrator Notebooks | [[Link](https://github.com/nhsx/au-azure-databricks/tree/main/orchestration)] | Azure Data Factory notebooks for running ephemeral job clusters |
| Function Notebooks     | [[Link](https://github.com/nhsx/au-azure-databricks/tree/main/functions)]               | Helper functions used across all notebooks                      |
| Analytics Notebooks    | [[Link](https://github.com/nhsx/au-azure-databricks/tree/main/analytics)]               | Analytical pipeline specific code                               |
| Ingestion Notebooks    | [[Link](https://github.com/nhsx/au-azure-databricks/tree/main/ingestion)]               | Data ingestion pipeline specific code                           |
| Debug Notebooks        | [[Link](https://github.com/nhsx/au-azure-databricks/tree/main/debug)]                   | For testing new code and reference                              |

### Built With

- [Databricks](https://databricks.com/)
- [Python 3](https://www.python.org/)
- [Azure](https://azure.microsoft.com/en-gb/)

<!-- GETTING STARTED -->

## Getting Started

To get a local copy up and running follow these simple steps.

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/nhsx/au-azure-databricks.git
   ```
2. Link to Azure Databricks, _see databricks [documentation](https://docs.databricks.com/notebooks/github-version-control.html)_

<!-- USAGE EXAMPLES -->

## Usage

Please refer to our [Read the Docs](https://nhsx.github.io/au-data-engineering/) site

<!-- ROADMAP -->

## Roadmap

See the [open issues](https://github.com/nhsx/au-azure-databricks/issues) for a list of proposed features (and known issues).

<!-- CONTRIBUTING-->

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

_See [CONTRIBUTING.md](https://github.com/nhsx/au-azure-databricks/blob/main/CONTRIBUTING.md) for detailed guidance._

<!-- LICENSE -->

## License

Distributed under the MIT License. _See [LICENSE.md](https://github.com/nhsx/au-azure-databricks/blob/main/LICENSE) for more information._

<!-- CONTACT -->

## About

Project contact email: [data@nhsx.nhs.uk](data@nhsx.nhs.uk)

To find out more about the [Analytics Unit](https://www.nhsx.nhs.uk/key-tools-and-info/nhsx-analytics-unit/) visit our [project website](https://nhsx.github.io/AnalyticsUnit/projects.html) or get in touch at [analytics-unit@nhsx.nhs.uk](mailto:data@nhsx.nhs.uk)

<!-- ACKNOWLEDGEMENTS
## Acknowledgements

* []()
* []()
* []() -->

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[contributors-shield]: https://img.shields.io/github/contributors/nhsx/au-azure-databricks.svg?color=blue&style=for-the-badge
[contributors-url]: https://github.com/nhsx/au-azure-databricks/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/nhsx/au-azure-databricks.svg?color=blue&style=for-the-badge
[forks-url]: https://github.com/nhsx/au-azure-databricks/network/members
[stars-shield]: https://img.shields.io/github/stars/nhsx/au-azure-databricks.svg?color=blue&style=for-the-badge
[stars-url]: https://github.com/nhsx/au-azure-databricks/stargazers
[issues-shield]: https://img.shields.io/github/issues/nhsx/au-azure-databricks.svg?color=blue&style=for-the-badge
[issues-url]: https://github.com/nhsx/au-azure-databricks/issues
[license-shield]: https://img.shields.io/github/license/nhsx/au-azure-databricks.svg?color=blue&style=for-the-badge
[license-url]: https://github.com/nhsx/au-azure-databricks/blob/main/LICENSE
[code-lines]: https://img.shields.io/tokei/lines/github/nhsx/au-azure-databricks?color=blue&label=Code%20Lines&style=for-the-badge
[code-lines-url]: https://github.com/nhsx/au-azure-databricks
