#!/usr/bin/env tclsh
# Minimal Tcl/Tk UI prototype for S.A.A.I.
package require Tk

set ::mqtt_host [expr {[info exists ::env(SAAI_MQTT_HOST)] ? $::env(SAAI_MQTT_HOST) : "localhost"}]
set ::mqtt_port [expr {[info exists ::env(SAAI_MQTT_PORT)] ? $::env(SAAI_MQTT_PORT) : 1883}]

wm title . "S.A.A.I. Orchestrator"
frame .top
pack .top -side top -fill x
label .top.status -text "MQTT: $::mqtt_host:$::mqtt_port"
pack .top.status -side left -padx 4 -pady 4

# Canvas placeholder for DAG rendering
canvas .graph -width 600 -height 400 -background #f5f5f5
pack .graph -fill both -expand 1 -padx 8 -pady 8

# Event list
text .log -width 80 -height 10
pack .log -fill both -expand 1 -padx 8 -pady 8

proc render_node {id x y status} {
    set color "#9ecaed"
    if {$status eq "RUNNING"} { set color "#7fc97f" }
    if {$status eq "FAILED"} { set color "#f0027f" }
    .graph create oval [expr {$x-25}] [expr {$y-25}] [expr {$x+25}] [expr {$y+25}] -fill $color -tags $id
    .graph create text $x $y -text $id
}

proc log_event {msg} {
    .log insert end "$msg\n"
    .log see end
}

log_event "UI initialized. Connect MQTT client to stream updates."

