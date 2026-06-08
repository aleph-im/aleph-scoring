{
  description = "Aleph Scoring - Node metrics collection and scoring";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    disko = {
      url = "github:nix-community/disko";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    disko,
  }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
    aleph-scoring = pkgs.callPackage ./default.nix {};
  in {
    packages.${system} = {
      default = aleph-scoring;
      docker-image = pkgs.dockerTools.buildLayeredImage {
        name = "registry.digitalocean.com/aleph-scoring/metrics";
        tag = "latest";
        contents = [aleph-scoring pkgs.cacert pkgs.coreutils pkgs.bash];
        config = {
          Entrypoint = ["${aleph-scoring}/bin/scoring"];
          Cmd = ["measure" "--publish"];
          Env = [
            "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            "ALEPH_SCORING_ASN_DB_DIRECTORY=/tmp/asn"
          ];
        };
      };
      digitalocean-image =
        self.nixosConfigurations.digitalocean.config.system.build.diskoImages;
    };

    nixosModules.default = import ./nix/service.nix;

    nixosConfigurations.digitalocean = nixpkgs.lib.nixosSystem {
      inherit system;
      modules = [
        disko.nixosModules.disko
        self.nixosModules.default
        ./nix/digitalocean.nix
        {
          services.aleph-scoring = {
            enable = true;
            package = aleph-scoring;
          };
        }
      ];
    };

    devShells.${system} = {
      default = pkgs.mkShell {
        name = "metrics-env";
        buildInputs = [pkgs.python3 aleph-scoring];
        ALEPH_SCORING_ASN_DB_DIRECTORY = "./asn";
        shellHook = ''
          echo "Run 'scoring measure' to measure the node metrics"
        '';
      };
      lab = import ./nix/shell-lab.nix {inherit pkgs;};
    };
  };
}
