//
// LibSourcey
// Copyright (C) 2005, Sourcey <http://sourcey.com>
//
// LibSourcey is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// LibSourcey is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//


#include "scy/pacm/installtask.h"
#include "scy/pacm/packagemanager.h"
#include "scy/pacm/package.h"
#include "scy/crypto/hash.h"
#include "scy/archo/zip.h"
#include "scy/http/authenticator.h"
#include "scy/http/client.h"
#include "scy/logger.h"
#include "scy/filesystem.h"

#include <boost/filesystem.hpp>

using namespace std;


namespace scy { 
namespace pacm {


InstallTask::InstallTask(PackageManager& manager, LocalPackage* local, RemotePackage* remote, 
						 const InstallOptions& options, uv::Loop* loop) :
	_manager(manager),
	_local(local),
	_remote(remote),
	_options(options),
	_dlconn(nullptr),
	_downloading(false),
	_progress(0),
	_loop(loop)
{
	TraceLS(this) << "Create" << endl;
	assert(valid());
}


InstallTask::~InstallTask()
{
	TraceLS(this) << "Destory" << endl;

	// :)
}


void InstallTask::start()
{
	TraceLS(this) << "Starting: Name=" << _local->name()
		<< ", Version= " << _options.version
		<< ", SDK Version=" << _options.sdkVersion
		<< endl;
		
	// Prepare environment and install options

	// Check against provided options to make sure that
	// we can proceed with task creation.
	if (!_options.version.empty())    { _remote->assetVersion(_options.version); } // throw if none
	if (!_options.sdkVersion.empty()) { _remote->latestSDKAsset(_options.sdkVersion); } // throw if none

	// Set default install directory if none was given
	if (_options.installDir.empty()) {

		// Use the current install dir if the local package already exists
		if (!_local->installDir().empty()) {
			_options.installDir = _local->installDir();
		}

		// Or use the manager default
		else {
			_options.installDir = _manager.options().installDir;
		}
	}

	// Normalize lazy windows paths
	_options.installDir = fs::normalize(_options.installDir);
	_local->setInstallDir(_options.installDir);

	// Create the directory
	fs::mkdirr(_options.installDir);

	// If the package failed previously we might need 
	// to clear the file cache.
	if (_manager.options().clearFailedCache)
		_manager.clearPackageCache(*_local);
	
	_runner.start(*this);

	// Increment the event loop while the task active
	_runner.handle().ref();
}


void InstallTask::cancel()
{
	setState(this, InstallationState::Cancelled, "Cancelled by user.");
}


void InstallTask::run()
{	
	try {
		auto local = this->local();
		switch (state().id()) 
		{			
		case InstallationState::None:			
			setProgress(0);
			doDownload();
			setState(this, InstallationState::Downloading);
			break;
		case InstallationState::Downloading:
			if (_downloading)
				return; // skip until download completes
			
			setState(this, InstallationState::Extracting);
			break;
		case InstallationState::Extracting:
			setProgress(75);
			doExtract();
			setState(this, InstallationState::Finalizing);
			break;
		case InstallationState::Finalizing:
			setProgress(90);
			doFinalize();
			local->setState("Installed");
			local->clearErrors();
			local->setInstalledAsset(getRemoteAsset());
			setProgress(100); // set before state change

			// Transition the internal state if finalization was a success.
			// This will complete the installation process.
			setState(this, InstallationState::Installed);
			break;
		case InstallationState::Installed:
			setComplete(); // complete and destroy
			return;
		case InstallationState::Cancelled:
			local->setState("Failed");
			setProgress(100);
			setComplete(); // complete and destroy
			return;			
		case InstallationState::Failed:
			local->setState("Failed");
			if (!state().message().empty())
				local->addError(state().message());
			setProgress(100); // complete and destroy
			setComplete();
			return;
		default: assert(0);
		}		
	}
	catch (std::exception& exc) {		
		ErrorL << "Installation failed: " << exc.what() << endl; 
		setState(this, InstallationState::Failed, exc.what());
	}
}


void InstallTask::onStateChange(InstallationState& state, const InstallationState& oldState)
{
	DebugL << "State changed: " << oldState << " => " << state << endl; 	

	// Set the package install task so we know from which state to
	// resume installation.
	// TODO: Should this be reset by the clearFailedCache option?
	local()->setInstallState(state.toString());

	Stateful<InstallationState>::onStateChange(state, oldState);
}


void InstallTask::doDownload()
{
	Package::Asset asset = getRemoteAsset();
	if (!asset.valid())
		throw std::runtime_error("Package download failed: The remote asset is invalid.");

	// If the remote asset already exists in the cache, we can 
	// skip the download. 
	/* // force file re-download until os get file size is fixed and we can match crc
	if (_manager.hasCachedFile(asset)) {
		DebugL << "file exists, skipping download" << endl;		
		setState(this, InstallationState::Extracting);
		return;
	}
	*/

	std::string outfile = _manager.getCacheFilePath(asset.fileName());
	auto url = _manager.options().endpoint + _manager.options().indexURI;
	auto i = url.find_last_of("/");
	url = url.substr(0, i + 1) + asset.fileName();
	_dlconn = http::Client::instance().createConnection(url, _loop);
	_dlconn->request().set("User-Agent", SCY_PACKAGE_STRING);
	if (!_manager.options().httpUsername.empty()) {
		http::BasicAuthenticator cred(
			_manager.options().httpUsername, 
			_manager.options().httpPassword);
		cred.authenticate(_dlconn->request()); 
	}
	
	DebugL << "Initializing download" 
		<< ": URI=" << url
		<< ", File path=" << outfile
		<< endl;

	_dlconn->setReadStream(new std::ofstream(outfile, std::ios_base::out | std::ios_base::binary));
	_dlconn->IncomingProgress += sdelegate(this, &InstallTask::onDownloadProgress);
	_dlconn->Complete += sdelegate(this, &InstallTask::onDownloadComplete);
	_dlconn->send();

	_downloading = true;
}


void InstallTask::onDownloadProgress(void*, const double& progress)
{
	DebugL << "Download progress: " << progress << endl;

	// Progress 1 - 75 covers download
	// Increments of 10 or greater
	int prog = static_cast<int>(progress * 0.75);
	if (prog > 0 && prog > this->progress() + 10)
		setProgress(prog);
}


void InstallTask::onDownloadComplete(void*, const http::Response& response)
{
	DebugL << "Download complete: " << response << endl;
	_dlconn->readStream<std::ofstream>()->close();
	_dlconn->close();
	_dlconn = nullptr;
	_downloading = false;
}


void InstallTask::doExtract()
{
	setState(this, InstallationState::Extracting);
		
	Package::Asset asset = getRemoteAsset();
	if (!asset.valid())
		throw std::runtime_error("The package can't be extracted");
	
	// Get the input file and check veracity
	std::string archivePath(_manager.getCacheFilePath(asset.fileName()));
	if (!fs::exists(archivePath))
		throw std::runtime_error("The local package file does not exist: " + archivePath);	
	if (!_manager.isSupportedFileType(asset.fileName()))
		throw std::runtime_error("The local package has an unsupported file extension: " + fs::extname(archivePath));

	// Verify file checksum if one was provided
	std::string originalChecksum(asset.checksum());
	if (!originalChecksum.empty()) {
		std::string computedChecksum(crypto::checksum(_manager.options().checksumAlgorithm, archivePath));		
		DebugL << "Verify checksum: original=" << originalChecksum << ", computed=" << computedChecksum << endl;
		if (originalChecksum != computedChecksum)
			throw std::runtime_error("Checksum verification failed: " + fs::extname(archivePath));
	}
	
	// Create the output directory
	std::string tempDir(_manager.getPackageDataDir(_local->id()));
	
	DebugL << "Unpacking archive: " << archivePath << " to " << tempDir << endl;

	// Reset the local installation manifest before extraction
	_local->manifest().root.clear();
	
	// Decompress the archive
	arc::ZipFile zip(archivePath);
	while (true) {
		zip.extractCurrentFile(tempDir, true);
	
		// Add the extracted file to the package install manifest	
		// Note: Manifest stores relative paths
		_local->manifest().addFile(zip.currentFileName()); 

		if (!zip.goToNextFile()) break;
	}
}

namespace bfs = boost::filesystem;

static bfs::path relativeTo(bfs::path from, bfs::path to)
{
   // Start at the root path and while they are the same then do nothing then when they first
   // diverge take the remainder of the two path and replace the entire from path with ".."
   // segments.
   bfs::path::const_iterator fromIter = from.begin();
   bfs::path::const_iterator toIter = to.begin();

   // Loop through both
   while (fromIter != from.end() && toIter != to.end() && (*toIter) == (*fromIter))
   {
      ++toIter;
      ++fromIter;
   }

   bfs::path finalPath;
   while (fromIter != from.end())
   {
      finalPath /= "..";
      ++fromIter;
   }

   while (toIter != to.end())
   {
      finalPath /= *toIter;
      ++toIter;
   }

   return finalPath;
}

void InstallTask::doFinalize() 
{
	setState(this, InstallationState::Finalizing);

	std::string tempDir(_manager.getPackageDataDir(_local->id()));
	std::string installDir = options().installDir;

	// Ensure the install directory exists
	fs::mkdirr(installDir);
	DebugL << "Finalizing to: " << installDir << endl;
	
	std::list<bfs::path> movedFiles;
	try {
		// Move all extracted files to the installation path
		auto iter = bfs::recursive_directory_iterator(tempDir);
		for (auto source : bfs::recursive_directory_iterator(tempDir)) {
			auto target = bfs::path(installDir) / relativeTo(tempDir, source);
			if (bfs::exists(target)) {
				if (bfs::is_directory(source) && bfs::is_directory(target)) {
					DebugL << "Skipping creation of existing directory " << target << endl;
				}
				else {
					throw std::runtime_error(target.string<std::string>() + " already exists");
				}
			}
			else {
				if (bfs::is_directory(source)) {
					bfs::create_directories(target);
				}
				else {
					DebugL << "Moving file: " << source << " => " << target << endl;
					bfs::rename(source, target);
					movedFiles.push_back(target);
				}
			}
		}
	}
	catch (std::exception& e) {
		// The previous version files may be currently in use,
		// in which case PackageManager::finalizeInstallations()
		// must be called from an external process before the
		// installation can be completed.
		ErrorL << "finalize error: " << e.what() << endl;

		DebugL << "Removing previously installed files" << endl;
		for (auto f : movedFiles) {
			boost::system::error_code ec;
			bfs::remove(f, ec);
			if (ec) {
				DebugL << "Error removing " << f << endl;
			}
		}

		DebugL << "Finalization failed, cancelling task" << endl;
		_local->addError(e.what());
		cancel();
		// The package requires finalizing at a later date. 
		// The current task will be cancelled, and the package
		// saved with the Installing state.
		throw;
	}
	
	// Remove the temporary output folder if the installation
	// was successfully finalized.
	try {		
		DebugL << "Removing temp directory: " << tempDir << endl;
		bfs::remove_all(tempDir);
#if 0
		// FIXME: How to remove a folder properly?
		fs::unlink(tempDir);
		assert(fs::exists(tempDir));
#endif
	}
	catch (std::exception& exc) {
		// While testing on a windows system this fails regularly
		// with a file sharing error, but since the package is already
		// installed we can just swallow it.
		WarnL << "cannot remove temp directory: " << exc.what() << endl;
	}

	DebugL << "finalization complete" << endl;
}


void InstallTask::setComplete()
{
	{
		//Mutex::ScopedLock lock(_mutex);
		assert(_progress == 100);

		InfoL << "Package installed: " 
			<< "Name=" << _local->name()
			<< ", Version=" << _local->version()
			<< ", Package State=" << _local->state()
			<< ", Package Install State=" << _local->installState()
			<< endl;
#ifdef _DEBUG
		_local->print(cout);	
#endif
	}

	// Close the connection
	{
		//Mutex::ScopedLock lock(_mutex);
		if (_dlconn)
			_dlconn->close();
	}

	// Cancel the runner and schedule for deletion
	assert(!_runner.cancelled());
	_runner.cancel();
	
	// The task will be destroyed
	// as a result of this signal.
	Complete.emit(this);
}


void InstallTask::setProgress(int value) 
{
	{
		//Mutex::ScopedLock lock(_mutex);	
		_progress = value;
	}
	Progress.emit(this, value);
}


Package::Asset InstallTask::getRemoteAsset() const
{
	//Mutex::ScopedLock lock(_mutex);
	return !_options.version.empty() ? 
		_remote->assetVersion(_options.version) : 
			!_options.sdkVersion.empty() ?
				_remote->latestSDKAsset(_options.sdkVersion) :
					_remote->latestAsset();
}


int InstallTask::progress() const
{
	//Mutex::ScopedLock lock(_mutex);
	return _progress;
}


bool InstallTask::valid() const
{
	//Mutex::ScopedLock lock(_mutex);
	return !stateEquals(InstallationState::Failed) 
		&& _local->valid() 
		&& (!_remote || _remote->valid());
}


bool InstallTask::cancelled() const
{
	return stateEquals(InstallationState::Cancelled);
}


bool InstallTask::failed() const
{
	return stateEquals(InstallationState::Failed);
}


bool InstallTask::success() const
{
	return stateEquals(InstallationState::Installed);
}


bool InstallTask::complete() const
{
	return stateEquals(InstallationState::Installed) 
		|| stateEquals(InstallationState::Cancelled) 
		|| stateEquals(InstallationState::Failed);
}


LocalPackage* InstallTask::local() const
{
	//Mutex::ScopedLock lock(_mutex);
	return _local;
}


RemotePackage* InstallTask::remote() const
{
	//Mutex::ScopedLock lock(_mutex);
	return _remote;
}


InstallOptions& InstallTask::options() 
{ 
	//Mutex::ScopedLock lock(_mutex);
	return _options;
}


uv::Loop* InstallTask::loop() const
{
	//Mutex::ScopedLock lock(_mutex);
	return _loop;
}


} } // namespace scy::pacm