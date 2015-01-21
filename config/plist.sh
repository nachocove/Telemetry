# Add the list of launchd jobs to this variable
jobs="
com.nachocove.dev.DailyMonitors
com.nachocove.alpha.DailyMonitors
com.nachocove.beta1.DailyMonitors
com.nachocove.beta1.SupportMonitor
com.nachocove.alpha.SupportMonitor
com.nachocove.beta1.EmailPerDomainReport
"

function install_plist {
  label=$1
  plist=~/Library/LaunchAgents/$label.plist
  echo "Install $label"
  if [ -f $plist ]
  then
    uninstall_plist_internal $1
  fi
  m4 -DREPO=$repo_path $label.template > $plist
  launchctl load $plist
}

function uninstall_plist_internal {
  plist=~/Library/LaunchAgents/$label.plist
  launchctl unload $plist
  rm -f $plist
}

function uninstall_plist {
  label=$1
  echo "Uninstall $label"
  uninstall_plist_internal $label
}
