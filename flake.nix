{
  description = "A flake to build moulinette";

  inputs = {
    utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nmattia/naersk";
  };

  outputs = { self, nixpkgs, utils, naersk }:
    utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages."${system}";
      naersk-lib = naersk.lib."${system}";
    in rec {
      # `nix build`
      packages.moulinette = naersk-lib.buildPackage {
        pname = "moulinette";
        root = ./.;
        buildInputs = with pkgs; [
          sqlite
        ];
      };
      defaultPackage = packages.moulinette;

      # `nix run`
      apps.moulinette = utils.lib.mkApp {
        drv = packages.moulinette;
      };
      defaultApp = apps.moulinette;

      # `nix develop`
      devShell = pkgs.mkShell {
        nativeBuildInputs = with pkgs; [ rustc cargo ];
      };
    });
}