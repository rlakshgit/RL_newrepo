query {claimsByDateRange (startDate: "start_date",
endDate: "end_date",
size:page_size,
page:page_count)  {
  claims{
    id
    external_claim_id
initiated_date
  updated_date
  updated_by {
    name
  }
  incident_type
  incident_damage_type
  claim_status
  communication_pref {
    contact_type
    contact_value
  }
  reporters {
    first_name
    last_name
    roles
    system_role
    address {
      address1
      address2
      city
      state_province
      postal_code
    }
    contacts {
      contact_type
      contact_value
    }
  }
  items {

		insured_item_id
                exposure {
                 exposure_id
                }
                type {name}

		reimbursement_forms {
		    reimbursement_form_logs {
               reimbursement_item_id
               dml_type
               dml_timestamp
               dml_created_by
               json_data
            }

			id
			vendor_organization_id
			reimbursement_items {
				status
				last_updated_at
				vendor_markup
				tax
				deductible
				shipping_fee
				reimbursement_costs {
					cost_type
					description
					amount
					}
				total_requested
				}

			payment_details {
				check_payable_to
				payment_type
				street
				city
				state
				postal_code
			}
		}
    type {
    name
      image_url
    active}
    created_ts
    updated_ts
    assignments {
      status
      organization {
        name
        origin_id
      }
      created_ts
      updated_ts
    }
    nick_name
  }
  documents {
    file_name
    file_type
    date_received
    sender {
      name
      zing_id
    }
    sender_role
    external_file_id
  }
  jeweler_access {
    status
    organization {
      name
      origin_id
    }
  }
  communication {
    conversation_type
    messages {
      message_type
      message_content
      created_ts
      sender{zing_id
            name
  organizations{name
  origin_id}
}

    }
    participants {
      name
      zing_id
      organizations {
        name
        origin_id
      }
    }
  }

}
size
page
count
}
}

