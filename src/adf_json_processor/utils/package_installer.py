import subprocess
import pkg_resources

class PackageInstaller:
    def __init__(self, package_name, repo_url):
        self.package_name = package_name
        self.repo_url = repo_url

    def is_package_installed(self, version=None):
        """Check if the package is installed, optionally verify its version."""
        try:
            pkg = pkg_resources.get_distribution(self.package_name)
            if version:
                return pkg.version == version
            return True
        except pkg_resources.DistributionNotFound:
            return False

    def get_installed_version(self):
        """Get the installed version of the package, if it exists."""
        try:
            pkg = pkg_resources.get_distribution(self.package_name)
            return pkg.version
        except pkg_resources.DistributionNotFound:
            return None

    def uninstall_existing_package(self):
        """Uninstall the existing package if needed."""
        print("Uninstalling the existing version...")
        try:
            subprocess.check_call(["pip", "uninstall", self.package_name, "-y"])
            print("Uninstallation completed successfully.")
        except subprocess.CalledProcessError as uninstall_error:
            installed_version = self.get_installed_version()
            if installed_version:
                print(f"Unable to uninstall package located outside the environment. Existing version is: {installed_version}")
            else:
                print("Error: The package could not be found for uninstallation.")
            print(f"Error during uninstallation: {uninstall_error}")
            raise SystemExit("Uninstallation failed. Aborting process.")

    def install_package_version(self, version):
        """Attempt to install the specified version of the package."""
        print(f"Attempting to install version {version}...")
        try:
            subprocess.check_call(["pip", "install", f"git+{self.repo_url}@{version}"])
            return version
        except subprocess.CalledProcessError as e:
            print(f"\nFailed to install version {version}. Error: {e}")
            return None

    def fallback_to_main_branch(self):
        """Fallback to installing from the main branch if the desired version fails."""
        print("Falling back to installing from the main branch instead...")
        try:
            subprocess.check_call(["pip", "install", f"git+{self.repo_url}@main"])
            return "main"
        except subprocess.CalledProcessError as fallback_error:
            print(f"\nInstallation from the main branch also failed. Error: {fallback_error}")
            raise SystemExit("Installation aborted due to repeated errors.")

    def verify_installation(self, desired_version, installed_version):
        """Verify if the installation was successful and print details."""
        try:
            package_info = pkg_resources.get_distribution(self.package_name)
            actual_installed_version = f"v{package_info.version}"  # Ensure "v" prefix is always present

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
        """Main method to manage the installation process."""
        version = f"v{version.lstrip('v')}"  # Normalize the version with "v" prefix

        if self.is_package_installed(version):
            print(f"The package {self.package_name} version {version} is already installed.")
            return

        if uninstall_existing:
            self.uninstall_existing_package()

        installed_version = self.install_package_version(version)
        if not installed_version:
            installed_version = self.fallback_to_main_branch()

        self.verify_installation(version, installed_version)