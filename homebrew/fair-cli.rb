class FairCli < Formula
    include Language::Python::Virtualenv
  
    desc "Synchronization interface for the SCRC FAIR Data Pipeline registry"
    homepage "https://www.fairdatapipeline.org/"
    url "https://files.pythonhosted.org/packages/20/c5/4f1e3fe03da7f957c99c70053e19a19a72d4d6e1c048ad94096597df169d/fair-cli-0.2.3.tar.gz"
    sha256 "92706fd21f8d97e2bd6bbdcd185aa0d6b6563bb4a53a29dbb4fc6c248c1959b8"
    license "BSD-2-Clause"
  
    depends_on "python3"
  
    def install
      virtualenv_create(libexec, "python3")
      virtualenv_install_with_resources
    end
  
    test do
      false
    end
  end
  