# Contributing to Transporter

Want to help build Trasporter? Great! This should help get you started
and hit us up if you have any questions.

## Topics

* [Architecture and Adaptor Enhancements](#architecture-and-adaptor-enhancements)
* [Reporting Issues](#reporting-issues)
* [Contribution Guidelines](#contribution-guidelines)

## Architecture and Adaptor Enhancements

When considering an architecture enhancement, we are looking for:

* A description of the problem this change solves
* An issue describing the design changes
  * Please prefix your issue with `Enhancement:` in the title
* Please review [the existing Enhancements](https://github.com/compose/transporter/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement)
  before reporting a new one.

When considering an adaptor enhancement, we are looking for:

* A description of the use case
  * Please note any logic changes if necessary
* A pull request with the code
  * Please prefix your PR's title with `Adaptor:` so we can quickly address it.
  * Your pull request must remain up to date with master, so rebase as necessary.

## Reporting Issues

A great way to contribute to the project is to send a detailed report when you
encounter an issue. We always appreciate a well-written, thorough bug report,
and will thank you for it!

When reporting [issues](https://github.com/compose/transporter/issues) on
GitHub please include your host OS (Ubuntu 12.04, Fedora 19, etc).
Please include:

* The output of `uname -a`.
* The output of `transporter version`.

Please also include the steps required to reproduce the problem if
possible and applicable.  This information will help us review and fix
your issue faster.

## Contribution guidelines

### Pull requests are always welcome

We are always excited to receive pull requests, and do our best to
process them as quickly as possible. Not sure if that typo is worth a pull
request? Do it! We will appreciate it.

If your pull request is not accepted on the first try, don't be
discouraged! If there's a problem with the implementation, hopefully you
received feedback on what to improve.

We're trying very hard to keep Transporter lean and so use of stdlib is encourage.
We don't want it to do everything for everybody. This means that we might decide against
incorporating a new feature. However, there might be a way to implement
that feature while still using Transporter.

### Create issues...

Any significant improvement should be documented as [a GitHub
issue](https://github.com/compose/transporter/issues) before anybody
starts working on it.

### ...but check for existing issues first!

Please take a moment to check that an issue doesn't already exist
documenting your bug report or improvement proposal. If it does, it
never hurts to add a quick "+1" or "I have this problem too". This will
help prioritize the most common problems and requests.

### Conventions

Fork the repository and make changes on your fork in a feature branch:

- If it's a bug fix branch, name it XXXX-something where XXXX is the number of the
  issue.
- If it's a feature branch, create an enhancement issue to announce your
  intentions, and name it XXXX-something where XXXX is the number of the issue.

Submit unit tests for your changes.  Go has a great test framework built in; use
it! Take a look at existing tests for inspiration. Run the full test suite (including
integration tests) on your branch before submitting a pull request.

Update the documentation when creating or modifying features.

Write clean code. Universally formatted code promotes ease of writing, reading,
and maintenance. Always run `gofmt -s -w file.go` on each changed file before
committing your changes. Most editors have plug-ins that do this automatically.

Pull requests descriptions should be as clear as possible and include a
reference to all the issues that they address.

Code review comments may be added to your pull request. Discuss, then make the
suggested modifications and push additional commits to your feature branch. Be
sure to post a comment after pushing. The new commits will show up in the pull
request automatically, but the reviewers will not be notified unless you
comment.

Pull requests must be cleanly rebased ontop of master without multiple branches
mixed into the PR.

**Git tip**: If your PR no longer merges cleanly, use `rebase master` in your
feature branch to update your pull request rather than `merge master`.

Before the pull request is merged, make sure that you squash your commits into
logical units of work using `git rebase -i` and `git push -f`. After every
commit the test suite should be passing. Include documentation changes in the
same commit so that a revert would remove all traces of the feature or fix.

Commits that fix or close an issue should include a reference like
`Closes #XXXX` or `Fixes #XXXX`, which will automatically close the
issue when merged.

Please do not add yourself to the `AUTHORS` file, as we will keep track of changes
and update the file when needed.
