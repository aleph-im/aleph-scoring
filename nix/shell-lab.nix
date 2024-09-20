{
  pkgs ? import <nixpkgs> { },
}:
let
in
pkgs.mkShell {
  name = "metrics-lab";

  buildInputs = [
    pkgs.python312Packages.jupyterlab
    pkgs.python312Packages.psycopg2
    pkgs.python312Packages.sqlalchemy
    pkgs.python312Packages.matplotlib
    pkgs.python312Packages.pandas
  ];

  shellHook = ''
    echo "Run 'scoring measure' to measure the node metrics"
  '';
}
