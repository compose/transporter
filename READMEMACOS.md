# Building Transporter with macOS

- follow instructions on http://golang.org/doc/install
- VERY IMPORTANT: Go has a required directory structure which the GOPATH needs to point to. Instructions can be found on http://golang.org/doc/code.html or by typing `go help gopath` in terminal.
- setup the directory structure in $GOPATH
    - `cd $GOPATH; mkdir src pkg bin`
    - create the github.com path and compose `mkdir -p src/github.com/compose; cd src/github.com/compose`
    - clone transporter `git clone https://github.com/compose/transporter; cd transporter`
    - install mods: `go mod download`
    - now you can build with `go build ./cmd/transporter/...`

At this point you should be able to run transporter via `$GOPATH/bin/transporter`,  you may need to add $GOPATH to your PATH environment variable. Something along the lines of `export PATH="$GOPATH/bin:$PATH"` should work.
