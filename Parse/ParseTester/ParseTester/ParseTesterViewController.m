//
//  ParseTesterViewController.m
//  ParseTester
//
//  Created by Henry Kwok on 5/6/14.
//  Copyright (c) 2014 Nacho Cove. All rights reserved.
//

#import "ParseTesterViewController.h"
#import "Parse/Parse.h"

@interface ParseTesterViewController ()

- (void)updateLoginInfo;
- (void)updateTextField:(UITextField *)textField text:(NSString *)text;

@end

@implementation ParseTesterViewController
- (void)viewDidLoad
{
    [super viewDidLoad];
	// Do any additional setup after loading the view, typically from a nib.
    [self updateLoginInfo];
    self.textNumber.delegate = self;
    self.textString.delegate = self;
    self.textListItem1.delegate = self;
    self.textListItem2.delegate = self;
    self.textObjectId.delegate = self;
}

- (void)didReceiveMemoryWarning
{
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

- (IBAction)logClicked:(id)sender {
    NSDictionary *params = @{ @"string": @"This is a string",
                                  @"integer": @"123"
                                 };
    // Send the dimensions to Parse along with the 'read' event
    
    [PFAnalytics trackEvent:@"Test" dimensions:params];
}

// Clicking each "label" toggles whether the field is included
// in the event. This is for testing Parse' ability to handle
// hetergenous data.
- (IBAction)numberClicked:(id)sender {
    self.textNumber.enabled = !self.textNumber.enabled;
}

- (IBAction)stringClicked:(id)sender {
    self.textString.enabled = !self.textString.enabled;
}

- (IBAction)booleanClicked:(id)sender {
    self.segCtrlBoolean.enabled = !self.segCtrlBoolean.enabled;
}

- (IBAction)listItem1Clicked:(id)sender {
    self.textListItem1.enabled = !self.textListItem1.enabled;
    self.textListItem2.enabled = !self.textListItem2.enabled;
}

- (IBAction)listItem2Clicked:(id)sender {
    self.textListItem1.enabled = !self.textListItem1.enabled;
    self.textListItem2.enabled = !self.textListItem2.enabled;
}

- (IBAction)objectIdEdited:(id)sender {
    // When an object id is entered, do a Parse query for the id.
    // This is for testing whether the ACL is working as expected.
    // The client should not be able to read any events. Including
    // the only created by itself.
    NSError *error = nil;
    PFQuery *query = [PFQuery queryWithClassName:@"Events"];
    PFObject *event = [query getObjectWithId:self.textObjectId.text error:&error];
    
    if (error) {
        NSLog(@"Query error (%@)", error);
    }
    
    [self updateTextField:self.textString
                     text:[event objectForKey:@"string"]];
    [self updateTextField:self.textNumber
                     text:[[event objectForKey:@"number"] stringValue]];
    // How do you update the boolean?
    NSArray *array = [event objectForKey:@"list"];
    if (nil == array) {
        [self updateTextField:self.textListItem1 text:nil];
        [self updateTextField:self.textListItem2 text:nil];
    } else {
        [self updateTextField:self.textListItem1 text:array[0]];
        [self updateTextField:self.textListItem2 text:array[1]];
    }
}

- (IBAction)logEventClicked:(id)sender {
    PFObject *obj = [PFObject objectWithClassName:@"Events"];
    if (self.textString.enabled) {
        obj[@"string"] = self.textString.text;
    }
    if (self.textNumber.enabled) {
        obj[@"number"] = [NSNumber numberWithInt:[self.textNumber.text intValue]];
    }
    if (self.segCtrlBoolean.enabled) {
        // What type is @YES?
        if (0 == self.segCtrlBoolean.selectedSegmentIndex) {
            obj[@"boolean"] = @NO;
        } else {
            obj[@"boolean"] = @YES;
        }
    }
    if (self.textListItem1.enabled) {
        assert(self.textListItem2.enabled);
        obj[@"list"] = @[self.textListItem1.text, self.textListItem2.text];
    }
    
    // Make it only readable by Ops role
    PFACL *acl = [PFACL ACL];
    [acl setWriteAccess:YES forRoleWithName:@"Ops"];
    obj.ACL = acl;
    
    // The official API does not like executing networking code in main thread.
    // I'm avoiding the callback and all that in a test program.
    [obj save];
    self.textObjectId.text = obj.objectId;
}

#pragma mark - ParseTesterViewController helpers

- (void)updateLoginInfo
{
    PFUser *user = [PFUser currentUser];
    self.textUsername.text = user.username;
    self.textPassword.text = user.password;
    self.textSessionToken.text = user.sessionToken;
    
    BOOL enabled = (1 == self.segCtrlLogin.selectedSegmentIndex);
    self.textUsername.enabled = enabled;
    self.textPassword.enabled = enabled;
    self.textSessionToken.enabled = enabled;
}

- (void)updateTextField:(UITextField *)textField text:(NSString *)text
{
    if (nil == text) {
        textField.text = nil;
        textField.enabled = false;
    } else {
        textField.enabled = true;
        textField.text = text;
    }
}

#pragma mark - UITextFieldDelegate

- (BOOL)textFieldShouldReturn:(UITextField *)textField
{
    [textField resignFirstResponder];
    return YES;
}

@end
