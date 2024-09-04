import subprocess
import pkg_resources

class PackageInstaller:
    def __init__(self, package_name, repo_url):
        """
        Initialize the PackageInstaller with the package name and repository URL.
        
        Args:
            package_name (str): The name of the package to be installed.
            repo_url (str): The Git repository URL where the package is hosted.
        """
        self.package_name = package_name
        self.repo_url = repo_url

    def uninstall_existing_package(self):
        """Uninstall the existing version of the package if installed."""
        print("Uninstalling the existing version...")
        try:
            subprocess.check_call(["pip", "uninstall", self.package_name, "-y"], shell=True)
            print("Uninstallation completed successfully.")
        except subprocess.CalledProcessError as uninstall_error:
            print(f"Error during uninstallation: {uninstall_error}")
            raise SystemExit("Uninstallation failed. Aborting process.")

    def install_package_version(self, version):
        """Attempt to install the specified version of the package from the repository."""
        print(f"Attempting to install version {version}...")
        try:
            subprocess.check_call([f"pip install git+{self.repo_url}@{version}"], shell=True)
            return version
        except subprocess.CalledProcessError as e:
            print(f"\nFailed to install version {version}. Error: {e}")
            return None

    def fallback_to_main_branch(self):
        """Fallback to installing from the main branch if the specified version fails."""
        print("Falling back to installing from the main branch instead...")
        try:
            subprocess.check_call([f"pip install git+{self.repo_url}@main"], shell=True)
            return "main"
        except subprocess.CalledProcessError as fallback_error:
            print(f"\nInstallation from the main branch also failed. Error: {fallback_error}")
            raise SystemExit("Installation aborted due to repeated errors.")

    def verify_installation(self, desired_version, installed_version):
        """Verify if the installation was successful and display relevant information."""
        try:
            package_info = pkg_resources.get_distribution(self.package_name)
            actual_installed_version = f"v{package_info.version}"

            print(f"\nInstallation successful.")
            print(f"Latest installed version: {actual_installed_version}")
            print(f"Installed package location: {package_info.location}")

            if installed_version != "main" and actual_installed_version != desired_version:
                print(f"\nWarning: The desired version was {desired_version}, but the installed version is {actual_installed_version}.")
        except pkg_resources.DistributionNotFound:
            print(f"\nInstallation completed, but the package '{self.package_name}' could not be found. Please check the installation.")
            raise SystemExit("Package not found after installation. Please verify the package name and installation steps.")
        finally:
            print("\nProcess completed. Ensure everything works as expected.")

    def install(self, version, uninstall_existing=False):
        """
        Manage the installation process, including uninstalling existing versions and verifying installation.
        
        Args:
            version (str): The version of the package to install.
            uninstall_existing (bool): Whether to uninstall the existing version first.
        """
        version = f"v{version.lstrip('v')}"  # Normalize the version with "v" prefix

        if uninstall_existing:
            self.uninstall_existing_package()

        installed_version = self.install_package_version(version)
        if not installed_version:
            installed_version = self.fallback_to_main_branch()

        self.verify_installation(version, installed_version)