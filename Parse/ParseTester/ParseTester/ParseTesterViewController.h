//
//  ParseTesterViewController.h
//  ParseTester
//
//  Created by Henry Kwok on 5/6/14.
//  Copyright (c) 2014 Nacho Cove. All rights reserved.
//

#import <UIKit/UIKit.h>

@interface ParseTesterViewController : UIViewController <UITextFieldDelegate>

// Login
@property (weak, nonatomic) IBOutlet UISegmentedControl *segCtrlLogin;

@property (weak, nonatomic) IBOutlet UILabel *labelUsername;
@property (weak, nonatomic) IBOutlet UILabel *labelPassword;
@property (weak, nonatomic) IBOutlet UILabel *labelSessionToken;

@property (weak, nonatomic) IBOutlet UITextField *textUsername;
@property (weak, nonatomic) IBOutlet UITextField *textPassword;
@property (weak, nonatomic) IBOutlet UITextField *textSessionToken;

@property (weak, nonatomic) IBOutlet UIButton *buttonLogin;

// Log Event
@property (weak, nonatomic) IBOutlet UIButton *buttonNumber;
@property (weak, nonatomic) IBOutlet UIButton *buttonString;
@property (weak, nonatomic) IBOutlet UIButton *buttonBoolean;
@property (weak, nonatomic) IBOutlet UIButton *buttonListItem1;
@property (weak, nonatomic) IBOutlet UIButton *buttonListItem2;
@property (weak, nonatomic) IBOutlet UIButton *buttonObjectId;

@property (weak, nonatomic) IBOutlet UITextField *textNumber;
@property (weak, nonatomic) IBOutlet UITextField *textString;
@property (weak, nonatomic) IBOutlet UITextField *textListItem1;
@property (weak, nonatomic) IBOutlet UISegmentedControl *segCtrlBoolean;
@property (weak, nonatomic) IBOutlet UITextField *textListItem2;
@property (weak, nonatomic) IBOutlet UITextField *textObjectId;

@property (weak, nonatomic) IBOutlet UIButton *buttonLogEvent;

- (IBAction)numberClicked:(id)sender;
- (IBAction)stringClicked:(id)sender;
- (IBAction)booleanClicked:(id)sender;
- (IBAction)listItem1Clicked:(id)sender;
- (IBAction)listItem2Clicked:(id)sender;
- (IBAction)objectIdEdited:(id)sender;

- (IBAction)logEventClicked:(id)sender;

@end
