

## Repository structure

The notes/ path has rough design notes and is not part of this repo. It also contains some useful papers. Do
not edit anything here.

The guides/ folder has guides about specific technical or other issues and coding guidelines.

The plans/ folder is for work plans. Remember to adjust these if during implementation things change and the
plan detail needs correcting.

Under tmp/ but not committed to repo are clones of dependencies so we can read code

## Dependencies

We are trying to not have too many dependencies and to keep code simple and understandable. Ask before adding
new dependencies.

## Testing

We are designing a highly reliable system so we need to have full trust in it. We need a very comprehensive
set of tests, and will look at different test methodologies, formal methods, fuzz testing and so on as needed.

We are using the Ceph test suite and porting these to native tests 1:1, these tests are in ./tmp/s3/tests

## Cleanliness

Make sure `cargo clippy` is clean, even if it is pedantic. Always run tests after making changes and make sure
they still pass. Review your code to make sure it is clear, correct and secure. Always run `cargo fmt`.

## Diary

We keep a diary os the work we did. This is a historical record, so only append to it. We will update this at
the end of the day, not after every work session.
