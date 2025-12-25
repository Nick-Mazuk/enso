{
  description = "shell";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      utils,
    }:
    let
      name = "shell";
    in
    utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShell = pkgs.mkShell {
          name = name;
          buildInputs = with pkgs; [
            bacon # for background rust code checking
            bun
            cargo
            protobuf
          ];
        };
      }
    );
}
