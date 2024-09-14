{ python3, pkgs }:

let
  aioipfs = python3.pkgs.buildPythonPackage rec {
    pname = "aioipfs";
    version = "0.4.4";

    src = python3.pkgs.fetchPypi {
      inherit pname version;
      sha256 = "sha256-KOp+2WY5m92vQLKeihfDH9hBfykYxPdAfxyTD4v0P3w=";
    };

    propagatedBuildInputs = with python3.pkgs; [
      aiohttp
      base58
      aiofiles
      py-multiaddr
    ];

    doCheck = true;
    pythonImportsCheck = [
      "aioipfs"
    ];
  };

  aleph-message = python3.pkgs.buildPythonPackage rec {
    pname = "aleph-message";
    version = "0.4.9";
    src = pkgs.fetchPypi {
      inherit version;
      pname = "aleph_message";
      sha256 = "sha256-CefACPgFMGG0F5lz/LdqkevrZsdd7afMNGjShlmSF7U=";
    };
    doCheck = false;
    pythonImportsCheck = [
      "aleph_message.models"
    ];

    propagatedBuildInputs = [
      pkgs.python3Packages.pydantic_1
    ];
  };

  aleph-superfluid = python3.pkgs.buildPythonPackage rec {
    pname = "aleph-superfluid";
    version = "0.2.1";
    src = pkgs.fetchPypi {
      inherit version;
      pname = "aleph_superfluid";
      sha256 = "sha256-6tiH5mNnIdAYCV40E70WH/v3CSlE0VI4WaHhkA/sZ0w=";
    };
    doCheck = false;
    pythonImportsCheck = [
      "superfluid"
    ];

    propagatedBuildInputs = [
      python3.pkgs.web3
    ];
  };

  aleph-sdk-python = python3.pkgs.buildPythonPackage rec {
    pname = "aleph-sdk-python";
    version = "1.0.1";
    pyproject = true;

    # The library should ideally be fetched from PyPI, but the latest version
    # has dependencies pinned too strictly for the propagated inputs from `nixpkgs`
    # to be satisfied. For now, we fetch the library from the branch
    # `hoh-relax-dependencies` on GitHub instead.
    #    src = pkgs.fetchPypi {
    #      inherit version;
    #      pname = "aleph_sdk_python";
    #      sha256 = "sha256-jsRT3dT3OgeNkDwGsYZc1kq2L26+HPuS1u/oYE9T3xk=";
    #    };
    src = pkgs.fetchFromGitHub {
      owner = "aleph-im";
      repo = "aleph-sdk-python";
      rev = "38a917d7664b47386da79f1b5bd1ef283e1aaaa2";
      sha256 = "sha256-Y/cXr64VYDo94RciVjveiZGaGdVHMqX2rQnORxD7R5g=";
    };

    doCheck = true;
    pythonImportsCheck = [
      "aleph.sdk"
    ];

    propagatedBuildInputs = [
      python3.pkgs.pydantic_1
      python3.pkgs.hatchling
      python3.pkgs.hatch-vcs
      python3.pkgs.aiohttp
      python3.pkgs.aioresponses
      python3.pkgs.coincurve
      python3.pkgs.eth-abi
      python3.pkgs.jwcrypto
      python3.pkgs.python-magic

      aleph-message
      aleph-superfluid
    ];
  };

  schedule = python3.pkgs.buildPythonPackage rec {
    pname = "schedule";
    version = "1.2.2";
    src = python3.pkgs.fetchPypi {
      inherit version;
      pname = "schedule";
      sha256 = "sha256-Ff6cdf5f2blifz8ZzA7xQgUI+fmkb0XNB2nvde3l8Lc=";
    };
    doCheck = false;
    propagatedBuildInputs = [ ];
  };
in
python3.pkgs.buildPythonPackage rec {
  pname = "aleph-scoring";
  version = "1.0.0"; # Define your version

  src = ./.;
  pyproject = true;

  nativeBuildInputs = with python3.pkgs; [
    hatchling
    hatch-vcs
  ] ++ [ pkgs.git ];

  propagatedBuildInputs = [
    python3.pkgs.asyncpg
    python3.pkgs.cachetools
    python3.pkgs.cryptography
    python3.pkgs.fusepy
    python3.pkgs.icmplib
    python3.pkgs.paramiko
    python3.pkgs.pyasn
    python3.pkgs.pygments
    python3.pkgs.python-magic
    python3.pkgs.sentry-sdk
    python3.pkgs.typer

    aioipfs
    aleph-sdk-python
    schedule
  ];

  doCheck = true;
  pythonImportsCheck = [
    "aleph_scoring"
    "aleph_scoring.metrics"
  ];

  meta = with python3.pkgs.lib; {
    description = "Aleph Scoring Project";
    homepage = "https://github.com/aleph-im/aleph-scoring";
    license = licenses.mit;
  };
}
