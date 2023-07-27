import * as dynamodb from "@aws-sdk/client-dynamodb";
import {
  CognitoIdentityProviderClient,
  AdminCreateUserCommand,
  AdminDeleteUserCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import * as ddb from "@aws-sdk/lib-dynamodb";
const doc_client = new dynamodb.DynamoDBClient({ region: "us-west-2" });
const cognito_client = new CognitoIdentityProviderClient({
  region: "us-west-2",
});
import { v4 as uuidv4 } from "uuid";

/*Environmental Variables*/
const bms_user_pool_id = process.env.Bms_User_Pool_Id;
const s3_bucket_name = process.env.S3_Bucket_Name;
const access_key = process.env.Access_Key;
const secret_key = process.env.Secret_Key;
const s3_region = process.env.S3_Region;

/*Dynamo SDK*/
const query_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.QueryCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const query_all_dynamo = async (params) => {
  try {
    let data = { Items: [], Count: 0 };
    const query_data = async (tableParams) => {
      const response = await doc_client.send(new ddb.QueryCommand(params));
      data.Count += response.Count;
      data.Items.push(...response.Items);
      if (response.LastEvaluatedKey) {
        tableParams.ExclusiveStartKey = response.LastEvaluatedKey;
        return await query_data(tableParams);
      } else {
        data.Count += response.Count;
        data.Items.push(...response.Items);
        return data;
      }
    };
    return await query_data(params);
  } catch (err) {
    console.error(err);
  }
};

const update_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.UpdateCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const insert_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.PutCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const delete_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.DeleteCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const scan_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.ScanCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const batch_insert_dynamo = async (params) => {
  try {
    const data = await doc_client.send(new ddb.BatchWriteCommand(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

/*Empty Fields*/
const check_empty_fields = (event) => {
  let checkEmptyFields = true;
  for (const field in event) {
    if (typeof event[field] == "string") {
      if (event[field].trim().length == 0) {
        checkEmptyFields = false;
      } else {
        event[field] = event[field].trim();
      }
    }
  }
  return checkEmptyFields;
};

/*Cognito SDK*/
const create_cognito_user = async (email_id) => {
  try {
    const createUserParams = {
      UserPoolId: bms_user_pool_id,
      Username: email_id,
      UserAttributes: [
        {
          Name: "email",
          Value: email_id,
        },
        {
          Name: "email_verified",
          Value: "true",
        },
      ],
      TemporaryPassword: (+Date.now()).toString(32),
    };
    const command = new AdminCreateUserCommand(createUserParams);
    await cognito_client.send(command);
    return "User Created Successfully";
  } catch (err) {
    console.error(err);
  }
};

const cognito_delete_user = async (email_id) => {
  try {
    const deleteUserCommand = {
      UserPoolId: bms_user_pool_id,
      Username: email_id,
    };
    const command = new AdminDeleteUserCommand(deleteUserCommand);
    await cognito_client.send(command);
    return "User Deleted Successfully";
  } catch (err) {
    console.error(err);
  }
};

/*Api's*/

/*-------- user api's --------*/
const create_user = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression: "user_email_id = :user_email_id",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.toLowerCase(),
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count == 0) {
      let createAdminParams = {
        TableName: "qr_jungle_bms_users",
        Item: {
          user_id: uuidv4(),
          user_email_id: event.user_email_id.toLowerCase(),
          user_status: "ACTIVE",
          user_created_on: Math.floor(new Date().getTime() / 1000),
          total_qrs_tagged: 0,
          total_qrs_created: 0,
        },
      };
      await create_cognito_user(event.user_email_id.toLowerCase());
      await insert_dynamo(createAdminParams);
      return {
        status: "Success",
        Status_message: "User Created Successfully!!",
      };
    } else {
      throw new Error(
        "User with Email id: " + event.user_email_id + " Already Exists"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Create User");
  }
};

const get_current_user_details = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.toLowerCase(),
        ":user_status": "ACTIVE",
      },
    };
    let userDetails = await query_dynamo(checkIfUserExistsParams);
    if (userDetails.Count > 0) {
      let response = {
        items: userDetails.Items,
        s3_details: {
          s3_bucket_name: s3_bucket_name,
          access_key: access_key,
          secret_key: secret_key,
          s3_region: s3_region,
        },
      };
      return response;
    } else {
      throw new Error(
        "User With Email Id: " + event.user_email_id + " Not Found"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot List User Details");
  }
};

const list_users = async (event) => {
  if (check_empty_fields(event)) {
    let listUsersParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_status-index",
      KeyConditionExpression: "user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_status": event.user_status,
      },
    };
    if (event.next_token != null && event.next_token != undefined) {
      listUsersParams.ExclusiveStartKey = JSON.parse(
        Buffer.from(event.next_token, "base64").toString("ascii")
      );
    }
    let usersDetails = await query_dynamo(listUsersParams);
    if (usersDetails.Count > 0) {
      let response = {
        items: usersDetails.Items.sort((a, b) =>
          a.user_email_id.localeCompare(b.user_email_id)
        ),
      };
      if (
        usersDetails.LastEvaluatedKey != undefined &&
        usersDetails.LastEvaluatedKey != null
      ) {
        response.next_token = Buffer.from(
          JSON.stringify(usersDetails.LastEvaluatedKey)
        ).toString("base64");
      }
      return response;
    } else {
      throw new Error("No Users Found");
    }
  } else {
    throw new Error("Empty Fields Occured Cannot List Users");
  }
};

const delete_user = async (event) => {
  if (check_empty_fields(event)) {
    if (event.user_email_id.trim() == "admin@qr.com") {
      throw new Error("Cannot Delete This Admin");
    } else {
      let checkIfUserExistsParams = {
        TableName: "qr_jungle_bms_users",
        IndexName: "user_email_id-user_status-index",
        KeyConditionExpression: "user_email_id = :user_email_id",
        ExpressionAttributeValues: {
          ":user_email_id": event.user_email_id.trim(),
        },
      };
      let user = await query_dynamo(checkIfUserExistsParams);
      if (user.Count > 0) {
        let deleteUserParams = {
          TableName: "qr_jungle_bms_users",
          Key: {
            user_id: user.Items[0].user_id,
          },
        };
        await cognito_delete_user(event.user_email_id.toLowerCase());
        await delete_dynamo(deleteUserParams);
        return {
          status: "Success",
          Status_message: "User Deleted Successfully!!",
        };
      } else {
        throw new Error(
          "User With Email Id: " + event.user_email_id + " Not Found"
        );
      }
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Delete User");
  }
};

/*-------- qr api's ---------*/
async function generate_qr_code() {
  const characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  async function generateString(length) {
    let result = "";
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    let checkIfCodeIsUniqueParams = {
      TableName: "qr_jungle_qrs",
      IndexName: "qr_code-index",
      KeyConditionExpression: "qr_code = :qr_code",
      ExpressionAttributeValues: {
        ":qr_code": result,
      },
    };
    let qr = await query_dynamo(checkIfCodeIsUniqueParams);
    if (qr.Count == 0) {
      return result.trim();
    } else {
      return await generateString(4);
    }
  }
  return await generateString(4);
}

const create_qrs = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let getDefaultQrCostParams = {
        TableName: "qr_jungle_qrs",
        KeyConditionExpression: "qr_id = :qr_id",
        ExpressionAttributeValues: {
          ":qr_id": "DEFAULT",
        },
      };
      let default_cost = await query_dynamo(getDefaultQrCostParams);
      if (default_cost.Count > 0) {
        let qrItems = [];
        let qrCount = event.no_of_qrs;
        for (let i = 0; i < qrCount; i++) {
          let qr_code = await generate_qr_code();
          qrItems.push({
            PutRequest: {
              Item: {
                qr_id: uuidv4(),
                qr_status: "UNTAGGED",
                qr_created_on: Math.floor(new Date().getTime() / 1000),
                qr_created_by: event.user_email_id.toLowerCase(),
                qr_redirect_url: "https://www.qrjungle.com",
                // qr_artistic_image: event.qr_artistic_image,
                qr_cost: default_cost.Items[0].qr_cost
                  ? default_cost.Items[0].qr_cost
                  : 0,
                qr_update: "True",
                qr_code,
              },
            },
          });
        }
        let batches = [];
        while (qrItems.length) {
          batches.push(qrItems.splice(0, 25));
        }
        for (var j = 0; j < batches.length; j++) {
          let batchwriteParams = {
            RequestItems: {
              qr_jungle_qrs: batches[j],
            },
          };
          await batch_insert_dynamo(batchwriteParams);
        }
        let updateQrCreatedCountParams = {
          TableName: "qr_jungle_bms_users",
          Key: {
            user_id: user.Items[0].user_id,
          },
          UpdateExpression: "ADD total_qrs_created :total_qrs_created",
          ExpressionAttributeValues: {
            ":total_qrs_created": event.no_of_qrs,
          },
        };
        await update_dynamo(updateQrCreatedCountParams);
        return {
          status: "Success",
          Status_message: "QR Created Successfully!!",
        };
      } else {
        throw new Error("Error In QR");
      }
    } else {
      throw new Error(
        "Active User With Email Id: " + event.user_email_id + " Not Found"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Create QR");
  }
};

const list_qrs = async (event) => {
  if (check_empty_fields(event)) {
    if (event.qr_status == "ALL") {
      let listAllQrDetailsParams = {
        TableName: "qr_jungle_qrs",
        FilterExpression: "qr_status <> :qr_status",
        ExpressionAttributeValues: {
          ":qr_status": "DEFAULT",
        },
      };
      if (event.next_token != null && event.next_token != undefined) {
        listAllQrDetailsParams.ExclusiveStartKey = JSON.parse(
          Buffer.from(event.next_token, "base64").toString("ascii")
        );
      }
      let all_qrs = await scan_dynamo(listAllQrDetailsParams);
      if (all_qrs.Count > 0) {
        let response = {
          items: all_qrs.Items.sort(
            (a, b) => b.qr_created_on - a.qr_created_on
          ),
        };
        if (
          all_qrs.LastEvaluatedKey != undefined &&
          all_qrs.LastEvaluatedKey != null
        ) {
          response.next_token = Buffer.from(
            JSON.stringify(all_qrs.LastEvaluatedKey)
          ).toString("base64");
        }
        return response;
      } else {
        throw new Error("No QR's To List!");
      }
    } else {
      let listQrDetailsParams = {
        TableName: "qr_jungle_qrs",
        IndexName: "qr_status-index",
        KeyConditionExpression: "qr_status = :qr_status",
        ExpressionAttributeValues: {
          ":qr_status": event.qr_status,
        },
      };
      if (event.next_token != null && event.next_token != undefined) {
        listQrDetailsParams.ExclusiveStartKey = JSON.parse(
          Buffer.from(event.next_token, "base64").toString("ascii")
        );
      }
      let qrs = await query_dynamo(listQrDetailsParams);
      if (qrs.Count > 0) {
        let response = {
          items: qrs.Items.sort((a, b) => {
            if (a.qr_purchased_on && b.qr_purchased_on) {
              return b.qr_purchased_on - a.qr_purchased_on;
            } else {
              return b.qr_created_on - a.qr_created_on;
            }
          }),
        };
        if (qrs.LastEvaluatedKey != undefined && qrs.LastEvaluatedKey != null) {
          response.next_token = Buffer.from(
            JSON.stringify(qrs.LastEvaluatedKey)
          ).toString("base64");
        }
        return response;
      } else {
        throw new Error("No " + event.qr_status + "  QR's To List!");
      }
    }
  } else {
    throw new Error("Empty Feilds Occured Cannot List QR's");
  }
};

const tag_qr = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfQrExistsParams = {
        TableName: "qr_jungle_qrs",
        KeyConditionExpression: "qr_id = :qr_id",
        FilterExpression: "qr_status = :qr_status",
        ExpressionAttributeValues: {
          ":qr_id": event.qr_id,
          ":qr_status": "UNTAGGED",
        },
      };
      let qr = await query_dynamo(checkIfQrExistsParams);
      if (qr.Count > 0) {
        let checkIfCategoryExistsParams = {
          TableName: "qr_jungle_categories",
          KeyConditionExpression: "category_id = :category_id",
          FilterExpression: "category_status = :category_status",
          ExpressionAttributeValues: {
            ":category_id": event.category_id,
            ":category_status": "ACTIVE",
          },
        };
        let category = await query_dynamo(checkIfCategoryExistsParams);
        if (category.Count > 0) {
          let getDefaultQrCostDetailsParams = {
            TableName: "qr_jungle_qrs",
            KeyConditionExpression: "qr_id = :qr_id",
            ExpressionAttributeValues: {
              ":qr_id": "DEFAULT",
            },
          };
          let default_cost = await query_dynamo(getDefaultQrCostDetailsParams);
          if (default_cost.Count > 0) {
            let default_qr_cost = default_cost.Items[0].qr_cost;
            let qr_discount = event.qr_discount ? event.qr_discount : 0;
            let tagQrParams = {
              TableName: "qr_jungle_qrs",
              Key: {
                qr_id: event.qr_id,
              },
              UpdateExpression:
                "SET qr_cost_currency_symbol = :qr_cost_currency_symbol,qr_cost_currency = :qr_cost_currency,category_name = :category_name,no_of_impressions = :no_of_impressions, qr_discounted_cost = :qr_discounted_cost, qr_status = :qr_status, qr_tagged_on = :qr_tagged_on, qr_tagged_by = :qr_tagged_by, qr_url = :qr_url, qr_category_id = :qr_category_id, qr_artistic_image = :qr_artistic_image,qr_discount = :qr_discount, qr_tagged_by_id = :qr_tagged_by_id",
              ExpressionAttributeValues: {
                ":qr_status": "TAGGED",
                ":qr_tagged_on": Math.floor(new Date().getTime() / 1000),
                ":qr_tagged_by": event.user_email_id.toLowerCase(),
                ":qr_url": `https://www.qrjungle.com/${qr.Items[0].qr_code}`,
                ":qr_category_id": event.category_id,
                ":qr_artistic_image": event.qr_artistic_image,
                ":qr_discount": qr_discount,
                ":qr_discounted_cost":
                  default_qr_cost - (qr_discount * default_qr_cost) / 100,
                ":qr_tagged_by_id": user.Items[0].user_id,
                ":no_of_impressions": 0,
                ":category_name": category.Items[0].category_name,
                ":qr_cost_currency": "USD",
                ":qr_cost_currency_symbol": "$",
              },
            };
            await update_dynamo(tagQrParams);
            let updateCategoryCountParmas = {
              TableName: "qr_jungle_categories",
              Key: {
                category_id: category.Items[0].category_id,
              },
              UpdateExpression: "ADD total_qr_ids :total_qr_ids",
              ExpressionAttributeValues: {
                ":total_qr_ids": 1,
              },
            };
            await update_dynamo(updateCategoryCountParmas);
            let updateBmsTagCountParams = {
              TableName: "qr_jungle_bms_users",
              Key: {
                user_id: user.Items[0].user_id,
              },
              UpdateExpression: "ADD total_qrs_tagged :total_qrs_tagged",
              ExpressionAttributeValues: {
                ":total_qrs_tagged": 1,
              },
            };
            await update_dynamo(updateBmsTagCountParams);
            return {
              status: "Success",
              Status_message: "QR Tagged Successfully!!",
            };
          } else {
            throw new Error("Error In QR");
          }
        } else {
          throw new Error(
            "Active Category With " + event.category_id + " Not Found!!"
          );
        }
      } else {
        throw new Error("UNTAGGED QR not found!!");
      }
    } else {
      throw new Error(
        "Active User With Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Tag QR!!");
  }
};

const untag_qr = async (event) => {
  if (check_empty_fields()) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfQrExistsParams = {
        TableName: "qr_jungle_qrs",
        KeyConditionExpression: "qr_id = :qr_id",
        FilterExpression: "qr_status = :qr_status",
        ExpressionAttributeValues: {
          ":qr_id": event.qr_id,
          ":qr_status": "TAGGED",
        },
      };
      let qr = await query_dynamo(checkIfQrExistsParams);
      if (qr.Count > 0) {
        let required_paramters = [
          "qr_id",
          "qr_code",
          "qr_created_by",
          "qr_created_on",
          "qr_redirect_url",
          "qr_status",
          "qr_cost",
          "qr_update",
        ];
        let unTagQrParmas = {
          TableName: "qr_jungle_qrs",
          Key: {
            qr_id: qr.Items[0].qr_id,
          },
          UpdateExpression:
            "REMOVE " +
            Object.keys(qr.Items[0])
              .filter((attribute) => !required_paramters.includes(attribute))
              .join(", ") +
            " SET  qr_status = :qr_status",
          ExpressionAttributeValues: {
            ":qr_status": "UNTAGGED",
          },
        };
        await update_dynamo(unTagQrParmas);
        let updateCategoryCountParmas = {
          TableName: "qr_jungle_categories",
          Key: {
            category_id: qr.Items[0].qr_category_id,
          },
          UpdateExpression: "ADD total_qr_ids :total_qr_ids",
          ExpressionAttributeValues: {
            ":total_qr_ids": -1,
          },
        };
        await update_dynamo(updateCategoryCountParmas);
        let updateBmsTagCountParams = {
          TableName: "qr_jungle_bms_users",
          Key: {
            user_id: user.Items[0].user_id,
          },
          UpdateExpression: "ADD total_qrs_tagged :total_qrs_tagged",
          ExpressionAttributeValues: {
            ":total_qrs_tagged": -1,
          },
        };
        await update_dynamo(updateBmsTagCountParams);
        return {
          status: "Success",
          Status_message: "QR Untagged Successfully!!",
        };
      } else {
        throw new Error("TAGGED QR Not Found!!");
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Delete QR!!");
  }
};

const update_tagged_qr_details = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfQrExistsParams = {
        TableName: "qr_jungle_qrs",
        KeyConditionExpression: "qr_id = :qr_id",
        FilterExpression: "qr_status = :qr_status",
        ExpressionAttributeValues: {
          ":qr_id": event.qr_id,
          ":qr_status": "TAGGED",
        },
      };
      let qr = await query_dynamo(checkIfQrExistsParams);
      if (qr.Count > 0) {
        let qr_cost = qr.Items[0].qr_cost;
        let qr_discount = event.qr_discount
          ? event.qr_discount
          : qr.Items[0].qr_discount;
        let getCategoryDetailsParams = {
          TableName: "qr_jungle_categories",
          KeyConditionExpression: "category_id = :category_id",
          FilterExpression: "category_status = :category_status",
          ExpressionAttributeValues: {
            ":category_id": event.category_id
              ? event.category_id
              : qr.Items[0].qr_category_id,
            ":category_status": "ACTIVE",
          },
        };
        let category = await query_dynamo(getCategoryDetailsParams);
        if (category.Count > 0) {
          let updateQrDetailsParams = {
            TableName: "qr_jungle_qrs",
            Key: {
              qr_id: event.qr_id,
            },
            UpdateExpression:
              "SET category_name = :category_name, qr_category_id = :qr_category_id, qr_discounted_cost = :qr_discounted_cost, qr_discount = :qr_discount, qr_updated_by = :qr_updated_by",
            ExpressionAttributeValues: {
              ":qr_category_id": event.category_id
                ? event.category_id
                : qr.Items[0].qr_category_id,
              ":qr_discounted_cost": qr_cost - (qr_discount * qr_cost) / 100,
              ":qr_discount": qr_discount,
              ":qr_updated_by": user.Items[0].user_email_id,
              ":category_name": event.category_id
                ? category.Items[0].category_name
                : qr.Items[0].category_name,
            },
            ReturnValues: "UPDATED_NEW",
          };
          await update_dynamo(updateQrDetailsParams);
          return {
            status: "Success",
            Status_message: "Updated QR Successfully!!",
          };
        } else {
          throw new Error("Active Category Not Found!!");
        }
      } else {
        throw new Error("TAGGED QR not found!!");
      }
    } else {
      throw new Error(
        "Active User With Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Cannot Update QR!!");
  }
};

const get_default_qr_cost = async (event) => {
  if (check_empty_fields(event)) {
    let getDefaultQrCostDetailsParams = {
      TableName: "qr_jungle_qrs",
      KeyConditionExpression: "qr_id = :qr_id",
      ExpressionAttributeValues: {
        ":qr_id": "DEFAULT",
      },
    };
    let qr = await query_dynamo(getDefaultQrCostDetailsParams);
    if (qr.Count > 0) {
      let response = {
        items: qr.Items,
      };
      return response;
    } else {
      throw new Error("Default QR Cost Doesn't Exists");
    }
  } else {
    throw new Error("Empty Fields Occured Cannot List Qr!!");
  }
};

const update_qr_cost = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id,
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkDefaultQrCostParams = {
        TableName: "qr_jungle_qrs",
        KeyConditionExpression: "qr_id = :qr_id",
        ExpressionAttributeValues: {
          ":qr_id": "DEFAULT",
        },
      };
      let default_qr = await query_dynamo(checkDefaultQrCostParams);
      if (default_qr.Count > 0) {
        let updateDefaultQRCostParams = {
          TableName: "qr_jungle_qrs",
          Key: {
            qr_id: "DEFAULT",
          },
          UpdateExpression: "SET qr_cost = :qr_cost",
          ExpressionAttributeValues: {
            ":qr_cost": event.qr_cost,
          },
        };
        await update_dynamo(updateDefaultQRCostParams);
        await new Promise((resolve) => setTimeout(resolve, 4000));
        return {
          status: "Success",
          Status_message: "QR Cost Updated Successfully!!",
        };
      } else {
        throw new Error("Default QR Cost Doesn't Exists");
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Update QR Details");
  }
};

/*-------- category api's --------*/
const create_category = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfCategoryExistsParams = {
        TableName: "qr_jungle_categories",
        IndexName: "category_name-index",
        KeyConditionExpression: "category_name = :category_name",
        ExpressionAttributeValues: {
          ":category_name": event.category_name,
        },
      };
      let category = await query_dynamo(checkIfCategoryExistsParams);
      if (category.Count == 0) {
        let createQrCategory = {
          TableName: "qr_jungle_categories",
          Item: {
            category_id: uuidv4(),
            category_status: "ACTIVE",
            category_name: event.category_name,
            category_created_on: Math.floor(new Date().getTime() / 1000),
            category_created_by: event.user_email_id.toLowerCase(),
            category_image: event.category_image,
            total_qr_ids: 0,
          },
        };
        await insert_dynamo(createQrCategory);
        return {
          status: "Success",
          Status_message: "Created Category Successfully!!",
        };
      } else {
        throw new Error(
          "Catergory " + event.category_name + " Already Exists!!"
        );
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Create Category");
  }
};

const list_categories = async (event) => {
  if (check_empty_fields(event)) {
    if (event.category_status == "ALL") {
      let listAllCategories = {
        TableName: "qr_jungle_categories",
        FilterExpression: "category_name <> :category_name",
        ExpressionAttributeValues: {
          ":category_name": "ALL",
        },
      };
      if (event.next_token != null && event.next_token != undefined) {
        listAllCategories.ExclusiveStartKey = JSON.parse(
          Buffer.from(event.next_token, "base64").toString("ascii")
        );
      }
      let all_categories = await scan_dynamo(listAllCategories);
      if (all_categories.Count > 0) {
        let response = {
          items: all_categories.Items.sort((a, b) =>
            a.category_name.localeCompare(b.category_name)
          ),
        };
        if (
          all_categories.LastEvaluatedKey != undefined &&
          all_categories.LastEvaluatedKey != null
        ) {
          response.next_token = Buffer.from(
            JSON.stringify(all_categories.LastEvaluatedKey)
          ).toString("base64");
        }
        return response;
      } else {
        throw new Error("No Categories to List!!");
      }
    } else {
      let checkIfCategoryExistsParams = {
        TableName: "qr_jungle_categories",
        IndexName: "category_status-index",
        KeyConditionExpression: "category_status = :category_status",
        ExpressionAttributeValues: {
          ":category_status": event.category_status,
        },
      };
      if (event.next_token != null && event.next_token != undefined) {
        checkIfCategoryExistsParams.ExclusiveStartKey = JSON.parse(
          Buffer.from(event.next_token, "base64").toString("ascii")
        );
      }
      let category = await query_dynamo(checkIfCategoryExistsParams);
      if (category.Count > 0) {
        let response = {
          items: category.Items.sort((a, b) =>
            a.category_name.localeCompare(b.category_name)
          ),
        };
        if (
          category.LastEvaluatedKey != undefined &&
          category.LastEvaluatedKey != null
        ) {
          response.next_token = Buffer.from(
            JSON.stringify(category.LastEvaluatedKey)
          ).toString("base64");
        }
        return response;
      } else {
        throw new Error("No Categories To List!!");
      }
    }
  } else {
    throw new Error("Empty Fields Occured Cannot List Categories");
  }
};

const active_deactive_catergory = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfCategoryExistsParams = {
        TableName: "qr_jungle_categories",
        KeyConditionExpression: "category_id = :category_id",
        ExpressionAttributeValues: {
          ":category_id": event.category_id,
        },
      };
      let category = await query_dynamo(checkIfCategoryExistsParams);
      if (category.Count > 0) {
        if (event.action == "DEACTIVATE") {
          if (
            category.Items[0].category_status == "ACTIVE" &&
            category.Items[0].total_qr_ids <= 0
          ) {
            let deactivateCategoryParams = {
              TableName: "qr_jungle_categories",
              Key: {
                category_id: event.category_id,
              },
              UpdateExpression: "SET category_status = :category_status",
              ExpressionAttributeValues: {
                ":category_status": "INACTIVE",
              },
              ReturnValues: "ALL_NEW",
            };
            await update_dynamo(deactivateCategoryParams);
            return {
              status: "Success",
              Status_message: "Deactivated Catergory Successfully!!",
            };
          } else {
            throw new Error("Unable To Deactive");
          }
        } else if (event.action == "ACTIVATE") {
          if (category.Items[0].category_status == "INACTIVE") {
            let deactivateCategoryParams = {
              TableName: "qr_jungle_categories",
              Key: {
                category_id: event.category_id,
              },
              UpdateExpression: "SET category_status = :category_status",
              ExpressionAttributeValues: {
                ":category_status": "ACTIVE",
              },
              ReturnValues: "ALL_NEW",
            };
            await update_dynamo(deactivateCategoryParams);
            return {
              status: "Success",
              Status_message: "Activated Catergory Successfully!!",
            };
          } else {
            throw new Error("Category Is Already INACTIVE");
          }
        }
      } else {
        throw new Error(
          "Category With Id " + event.category_id + " Not Found!!"
        );
      }
    } else {
      throw new Error(
        "Active User With Email " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Fields Cannot Be Empty!!");
  }
};

const delete_category = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.toLowerCase(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfCategoryExistsParams = {
        TableName: "qr_jungle_categories",
        KeyConditionExpression: "category_id = :category_id",
        FilterExpression: "total_qr_ids <= :total_qr_ids",
        ExpressionAttributeValues: {
          ":category_id": event.category_id,
          ":total_qr_ids": 0,
        },
      };
      let category = await query_dynamo(checkIfCategoryExistsParams);
      if (category.Count > 0) {
        let deleteCategoryParams = {
          TableName: "qr_jungle_categories",
          Key: {
            category_id: category.Items[0].category_id,
          },
        };
        await delete_dynamo(deleteCategoryParams);
        return {
          status: "Success",
          Status_message: "Category Deleted Successfully",
        };
      } else {
        throw new Error("Cannot Delete Category, Contains 1 or more QR's!!");
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Delete Catergory!!");
  }
};

/*-------- portal side -------*/
const list_consumer_users = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.toLowerCase(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let getConsumerDetailsParams = {
        TableName: "qr_jungle_portal_users",
      };
      let consumer_users = await scan_dynamo(getConsumerDetailsParams);
      if (consumer_users.Count > 0) {
        let response = {
          items: consumer_users.Items.sort(
            (a, b) => b.no_of_qr_purchased - a.no_of_qr_purchased
          ),
        };
        return response;
      } else {
        throw new Error("No Users Found!!");
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot Delete Catergory!!");
  }
};

const list_consumer_qrs = async (event) => {
  if (check_empty_fields(event)) {
    let checkIfUserExistsParams = {
      TableName: "qr_jungle_bms_users",
      IndexName: "user_email_id-user_status-index",
      KeyConditionExpression:
        "user_email_id = :user_email_id AND user_status = :user_status",
      ExpressionAttributeValues: {
        ":user_email_id": event.user_email_id.trim(),
        ":user_status": "ACTIVE",
      },
    };
    let user = await query_dynamo(checkIfUserExistsParams);
    if (user.Count > 0) {
      let checkIfConsumerExistsParmas = {
        TableName: "qr_jungle_portal_users",
        IndexName: "user_email_id-user_status-index",
        KeyConditionExpression:
          "user_email_id = :user_email_id AND user_status = :user_status",
        ExpressionAttributeValues: {
          ":user_email_id": event.consumer_user_email_id.trim(),
          ":user_status": "ACTIVE",
        },
      };
      let consumer_user = await query_dynamo(checkIfConsumerExistsParmas);
      if (consumer_user.Count > 0) {
        let checkIfConsumerQrExistsParams = {
          TableName: "qr_jungle_qrs",
          IndexName: "qr_purchased_by-qr_status-index",
          KeyConditionExpression:
            "qr_purchased_by = :qr_purchased_by AND qr_status = :qr_status",
          ExpressionAttributeValues: {
            ":qr_purchased_by": event.consumer_user_email_id,
            ":qr_status": "PURCHASED",
          },
        };
        let consumer_qrs = await query_dynamo(checkIfConsumerQrExistsParams);
        if (consumer_qrs.Count > 0) {
          let response = {
            items: consumer_qrs.Items,
          };
          return response;
        } else {
          throw new Error("There Are No QR's To List");
        }
      } else {
        throw new Error(
          "Active Consumer with Email Id: " +
            event.consumer_user_email_id +
            " Not Found!!"
        );
      }
    } else {
      throw new Error(
        "Active User with Email Id: " + event.user_email_id + " Not Found!!"
      );
    }
  } else {
    throw new Error("Empty Fields Occured Cannot List Qr!!");
  }
};

const test = async (event) => {
  const utcTime = new Date(Math.floor(new Date().getTime() / 1000));
  const istOffset = 60 * 60;
  const istTime = new Date(utcTime.getTime() + istOffset).toLocaleString(
    "en-IN",
    { timeZone: "Asia/Kolkata" }
  );

  console.log("hi", istTime);
};

/*Handler*/
export const handler = async (event) => {
  console.log(JSON.stringify(event));
  try {
    switch (event.command) {
      case "test":
        return await test(event);

      case "createUser":
        return await create_user(event);

      case "getCurrentUserDetails":
        return await get_current_user_details(event);

      case "listUsers":
        return await list_users(event);

      case "deleteUser":
        return await delete_user(event);

      case "createQrs":
        return await create_qrs(event);

      case "listQrs":
        return await list_qrs(event);

      case "tagQr":
        return await tag_qr(event);

      case "untagQr":
        return await untag_qr(event);

      case "updateTaggedQrDetails":
        return await update_tagged_qr_details(event);

      case "getDefaultQrCost":
        return await get_default_qr_cost(event);

      case "updateQrCost":
        return await update_qr_cost(event);

      case "createCategory":
        return await create_category(event);

      case "listCategories":
        return await list_categories(event);

      case "activeDeactiveCatergory":
        return await active_deactive_catergory(event);

      case "deleteCategory":
        return await delete_category(event);

      case "listConsumerUsers":
        return await list_consumer_users(event);

      case "listConsumerQrs":
        return await list_consumer_qrs(event);

      default:
        return "Command not found";
    }
  } catch (err) {
    throw new Error(err);
  }
};
