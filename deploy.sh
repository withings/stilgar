#!/bin/bash

if systemctl is-active -q stilgar; then
    sudo systemctl restart stilgar
else
    true
fi
